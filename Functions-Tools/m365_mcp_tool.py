"""
title: MS365 MCP (Single-Auth Proxy)
author: IT Systems Team
description: Provides single-sign-on access for Microsoft 365 Graph tools through the local MCP server. Normalizes arguments, retries once on schema errors, resolves common IDs, and converts common AQS searches to OData filters for Mail. Hardened select/orderby to avoid OData 400s.
version: 1.0.0
"""

import os
import time
import uuid
import json
import re
import requests
from typing import Any, Dict, Optional, List, Tuple


def _coerce_datetime_block(value: Any) -> Optional[Dict[str, str]]:
    """Convert simple datetime representations to the Graph {dateTime,timeZone} shape."""
    if isinstance(value, dict):
        dt = value.get("dateTime") or value.get("datetime") or value.get("value")
        tz = value.get("timeZone") or value.get("timezone") or value.get("tz")
        if dt:
            return {"dateTime": str(dt), "timeZone": str(tz or "UTC")}
        return None
    if isinstance(value, str):
        return {"dateTime": value, "timeZone": "UTC"}
    return None


def _normalize_item_body(value: Any, default_type: str = "text") -> Dict[str, Any]:
    """Ensure Graph itemBody objects use the expected {content, contentType} shape."""
    if isinstance(value, dict) and "body" in value and len(value) == 1:
        # Collapse {"body": {...}} shapes that sometimes show up
        return _normalize_item_body(value["body"], default_type)

    content: Optional[str] = None
    content_type: Optional[str] = None

    if isinstance(value, dict):
        if "html" in value and "content" not in value:
            content = str(value.get("html"))
            content_type = "html"
        if "text" in value and content is None:
            content = str(value.get("text"))
            content_type = content_type or "text"
        if "content" in value:
            content = str(value.get("content"))
        if "contentType" in value:
            content_type = str(value.get("contentType"))
        if "type" in value and content_type is None:
            content_type = str(value.get("type"))
    elif isinstance(value, str):
        content = value
    elif value is None:
        content = ""
    else:
        content = json.dumps(value)

    if not content_type:
        content_type = default_type

    content_type = content_type.lower()
    if content_type in {"plaintext", "plain"}:
        content_type = "text"
    elif content_type in {"richtext", "rich-text", "rtf"}:
        content_type = "html"
    elif content_type.endswith("/html"):
        content_type = "html"
    elif content_type not in {"text", "html"}:
        content_type = default_type

    if content is None:
        content = ""

    return {"content": content, "contentType": content_type}


def _coerce_email_address(value: Any) -> Optional[Dict[str, Any]]:
    """Normalize various recipient representations to {emailAddress:{address,name?}}."""
    if isinstance(value, dict):
        if "emailAddress" in value and isinstance(value["emailAddress"], dict):
            address = value["emailAddress"].get("address") or value["emailAddress"].get(
                "email"
            )
            if not address:
                return None
            entry = {"address": str(address)}
            name = (
                value["emailAddress"].get("name")
                or value.get("name")
                or value.get("displayName")
            )
            if name:
                entry["name"] = str(name)
            return {"emailAddress": entry}
        address = (
            value.get("address")
            or value.get("email")
            or value.get("mail")
            or value.get("userPrincipalName")
        )
        if not address:
            return None
        entry = {"address": str(address)}
        name = value.get("name") or value.get("displayName")
        if name:
            entry["name"] = str(name)
        return {"emailAddress": entry}
    if isinstance(value, str):
        address = value.strip()
        if not address:
            return None
        return {"emailAddress": {"address": address}}
    return None


def _normalize_recipients(value: Any) -> List[Dict[str, Any]]:
    """Accept strings, dicts, and nested lists for recipients and dedupe by address."""
    if value is None:
        return []
    items: List[Any]
    if isinstance(value, list):
        items = value
    else:
        items = [value]

    collected: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, list):
            collected.extend(_normalize_recipients(item))
            continue
        coerced = _coerce_email_address(item)
        if coerced:
            collected.append(coerced)

    seen: set = set()
    deduped: List[Dict[str, Any]] = []
    for entry in collected:
        address = entry.get("emailAddress", {}).get("address")
        if address:
            key = address.strip().lower()
            if key in seen:
                continue
            seen.add(key)
        deduped.append(entry)
    return deduped


def _normalize_message_payload(value: Any) -> Dict[str, Any]:
    """Coerce mail message payloads so they meet the Graph message schema."""
    if isinstance(value, dict):
        message = dict(value)
    elif isinstance(value, str):
        message = {
            "body": _normalize_item_body(value, "html" if "<" in value else "text")
        }
    elif value is None:
        message = {}
    else:
        message = {"body": _normalize_item_body(value)}

    def _pop_any(src: Dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in src:
                return src.pop(name)
        return None

    if "body" in message:
        message["body"] = _normalize_item_body(
            message["body"],
            (
                "html"
                if isinstance(message["body"], str) and "<" in message["body"]
                else "text"
            ),
        )
    else:
        body_fields: Dict[str, Any] = {}
        content = _pop_any(message, "html", "text", "content")
        if content is not None:
            body_fields["content"] = content
        content_type = _pop_any(message, "contentType", "bodyType")
        if content_type is not None:
            body_fields["contentType"] = content_type
        if body_fields:
            message["body"] = _normalize_item_body(body_fields)

    for field, aliases in (
        ("toRecipients", ("toRecipients", "to", "recipients")),
        ("ccRecipients", ("ccRecipients", "cc")),
        ("bccRecipients", ("bccRecipients", "bcc")),
        ("replyTo", ("replyTo", "replyToRecipients")),
    ):
        raw = _pop_any(message, *aliases)
        if raw is not None:
            normalized = _normalize_recipients(raw)
            if normalized:
                message[field] = normalized

    if "from" in message:
        from_normalized = _normalize_recipients(message["from"])
        if from_normalized:
            message["from"] = from_normalized[0]
        else:
            message.pop("from", None)

    attachments = message.get("attachments")
    if attachments is not None:
        if not isinstance(attachments, list):
            attachments = [attachments]
        normalized_attachments = [
            item for item in attachments if isinstance(item, dict)
        ]
        if normalized_attachments:
            message["attachments"] = normalized_attachments
        else:
            message.pop("attachments", None)

    for key in ("importance", "sensitivity"):
        if key in message and isinstance(message[key], str):
            message[key] = message[key].lower()

    return message


def _normalize_todo_patch(value: Any) -> Dict[str, Any]:
    """Normalize To Do create/update payloads, especially body and datetime shapes."""
    if isinstance(value, dict):
        patch = dict(value)
    elif isinstance(value, str):
        patch = {"body": value}
    elif value is None:
        patch = {}
    else:
        patch = {"body": value}

    if "body" in patch:
        patch["body"] = _normalize_item_body(patch["body"])
    else:
        simple_body_keys = {"content", "contentType", "text", "html"}
        if simple_body_keys.intersection(patch.keys()):
            body_fields: Dict[str, Any] = {}
            if "html" in patch:
                body_fields["content"] = patch.pop("html")
                body_fields["contentType"] = "html"
            if "text" in patch and "content" not in body_fields:
                body_fields["content"] = patch.pop("text")
            if "content" in patch and "content" not in body_fields:
                body_fields["content"] = patch.pop("content")
            if "contentType" in patch:
                body_fields["contentType"] = patch.pop("contentType")
            patch["body"] = _normalize_item_body(body_fields)

    categories = patch.get("categories")
    if isinstance(categories, str):
        patch["categories"] = [
            c.strip() for c in re.split(r"[;,]", categories) if c.strip()
        ]
    elif isinstance(categories, list):
        patch["categories"] = [str(c).strip() for c in categories if str(c).strip()]

    for dt_key in (
        "dueDateTime",
        "reminderDateTime",
        "startDateTime",
        "completedDateTime",
    ):
        if dt_key in patch:
            coerced = _coerce_datetime_block(patch[dt_key])
            if coerced:
                patch[dt_key] = coerced
            else:
                patch.pop(dt_key, None)

    return patch


def _parse_event_stream_json(text: Any) -> Optional[Any]:
    # Parse event-stream snippets into JSON where possible.
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except Exception:
        pass

    chunks: List[str] = []
    for line in stripped.splitlines():
        line = line.strip()
        if not line.lower().startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload:
            chunks.append(payload)

    for chunk in reversed(chunks):
        try:
            return json.loads(chunk)
        except Exception:
            continue

    return None


def _unwrap_streaming_result(data: Any) -> Any:
    # Unpack streamed MCP responses into their JSON payloads.
    if isinstance(data, dict):
        if "raw" in data:
            parsed = _parse_event_stream_json(data.get("raw"))
            if parsed is not None:
                return _unwrap_streaming_result(parsed)
        if "result" in data:
            return _unwrap_streaming_result(data["result"])
        content = data.get("content")
        if isinstance(content, list):
            for item in content:
                if not isinstance(item, dict):
                    continue
                parsed = _parse_event_stream_json(item.get("text"))
                if parsed is not None:
                    return _unwrap_streaming_result(parsed)
        return data
    if isinstance(data, list):
        return data
    if isinstance(data, str):
        parsed = _parse_event_stream_json(data)
        if parsed is not None:
            return _unwrap_streaming_result(parsed)
    return data


def _normalize_exclusion_phrases(aqs: Any) -> Any:
    """Convert plain-language exclude/except phrases into NOT from clauses."""
    if not isinstance(aqs, str):
        return aqs

    def _wrap(target: str) -> str:
        target = target.strip()
        if not target:
            return target
        if target.startswith('"') and target.endswith('"'):
            return target
        return f'"{target}"'

    s = aqs
    patterns = [
        re.compile(
            r"\b(?:exclude|excluding|except|without)\s+(?:messages?\s+)?from:\s*(\"[^\"]+\"|[^\s]+)",
            flags=re.I,
        ),
        re.compile(
            r"\b(?:exclude|excluding|except|without)\s+(\"[^\"]+\"|[^\s]+@[^\s]+)",
            flags=re.I,
        ),
    ]

    for pattern in patterns:

        def _repl(match: re.Match) -> str:
            token = (match.group(1) or "").strip()
            if not token:
                return match.group(0)
            lowered = token.lower()
            if lowered.startswith("not") and "from:" in lowered:
                return match.group(0)
            return f" NOT from:{_wrap(token)} "

        s = pattern.sub(_repl, s)

    return re.sub(r"\s+", " ", s).strip()


def _pull_folder_from_aqs(aqs: Any) -> Tuple[str, Optional[str]]:
    # Extract folder:name clauses from an AQS string.
    if not isinstance(aqs, str):
        return aqs, None

    folder_name: Optional[str] = None

    def _repl(match: re.Match) -> str:
        nonlocal folder_name
        if folder_name is None:
            candidate = match.group(1) or match.group(2) or ""
            candidate = candidate.strip()
            if (
                candidate.startswith('"')
                and candidate.endswith('"')
                and len(candidate) > 1
            ):
                candidate = candidate[1:-1]
            folder_name = candidate
        return " "

    pattern = re.compile(r'\bfolder\s*:\s*(?:"([^"]+)"|([^\s]+))', flags=re.I)
    cleaned = pattern.sub(_repl, aqs)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned, folder_name


# Toggle runtime logging without redeploying:
#   - Set env OPENWEBUI_TOOL_DEBUG=1 to enable
DEBUG = os.getenv("OPENWEBUI_TOOL_DEBUG", "0") == "1"
# How many characters to show from payload/result in logs
LOG_SNIP = int(os.getenv("OPENWEBUI_TOOL_LOG_SNIP", "1200"))

# Allowed MCP tool names (expand as needed)
ALLOWED_TOOLS = {
    # Calendar
    "list-calendars",
    "list-calendar-events",
    "get-calendar-event",
    "get-calendar-view",
    "create-calendar-event",
    "update-calendar-event",
    "delete-calendar-event",
    # Mail
    "list-mail-folders",
    "list-mail-folder-messages",
    "list-mail-messages",
    "get-mail-message",
    "send-mail",
    "create-draft-email",
    "move-mail-message",
    "delete-mail-message",
    # To Do
    "list-todo-task-lists",
    "list-todo-tasks",
    "get-todo-task",
    "create-todo-task",
    "update-todo-task",
    "delete-todo-task",
    # Planner
    "list-planner-tasks",
    "list-plan-tasks",
    "get-planner-plan",
    "get-planner-task",
    "create-planner-task",
    # OneNote
    "list-onenote-notebooks",
    "list-onenote-notebook-sections",
    "list-onenote-section-pages",
    "get-onenote-page-content",
    "create-onenote-page",
    # Excel
    "list-excel-worksheets",
    "get-excel-range",
    "create-excel-chart",
    "format-excel-range",
    "sort-excel-range",
    # OneDrive / Files
    "list-drives",
    "get-drive-root-item",
    "list-folder-files",
    "download-onedrive-file-content",
    "upload-file-content",
    "upload-new-file",
    "delete-onedrive-file",
    # Contacts
    "list-outlook-contacts",
    "get-outlook-contact",
    "create-outlook-contact",
    "update-outlook-contact",
    "delete-outlook-contact",
    # Teams / Org mode
    "list-joined-teams",
    "get-team",
    "list-team-channels",
    "get-channel-message",
    "send-channel-message",
    # SharePoint / Org mode
    "search-sharepoint-sites",
    "list-sharepoint-site-items",
    "list-sharepoint-site-lists",
    "get-sharepoint-site-list-item",
    "list-sharepoint-site-drives",
    # Misc
    "get-current-user",
    "search-query",
}

# Soft schema hints used by the normalizer (applies across many tools)
ARRAY_KEYS = {"orderby", "select", "expand"}  # only these get list coercion
ALIAS_KEYS = {
    "orderBy": "orderby",
    "$filter": "filter",
    # To Do param aliasing: some callers send taskListId; MCP expects todoTaskListId
    "taskListId": "todoTaskListId",
    "tasklistid": "todoTaskListId",
    "TaskListId": "todoTaskListId",
    "taskId": "todoTaskId",
    "taskID": "todoTaskId",
    "TaskId": "todoTaskId",
    "TaskID": "todoTaskId",
    "taskid": "todoTaskId",
    "todoTaskID": "todoTaskId",
}

# ---- Safe fields/clauses to prevent OData 400s ----
SAFE_SELECTS = {
    "list-todo-task-lists": {"id", "displayName", "wellknownListName"},
    "list-todo-tasks": {
        "id",
        "title",
        "status",
        "importance",
        "createdDateTime",
        "dueDateTime",
        "isReminderOn",
        "reminderDateTime",
        "categories",
    },
}
SAFE_ORDERBY = {
    # For tasks, Graph often rejects nested paths like "dueDateTime/dateTime".
    # Keep only simple fields. Sort due dates client-side if needed.
    "list-todo-tasks": {
        "createdDateTime asc",
        "createdDateTime desc",
        "lastModifiedDateTime asc",
        "lastModifiedDateTime desc",
        "status asc",
        "status desc",
        "importance asc",
        "importance desc",
    },
}


def _safelog(prefix: str, obj: Any) -> None:
    """Pretty-print JSON with truncation; never include secrets."""
    if not DEBUG:
        return
    try:
        text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        text = str(obj)
    if len(text) > LOG_SNIP:
        text = text[:LOG_SNIP] + f"... [snip {len(text)-LOG_SNIP} chars]"
    print(f"[MS365-MCP] {prefix}: {text}")


def _extract_items_flex(result: Any) -> List[Dict[str, Any]]:
    """
    Try multiple common shapes to find a list of items:
    - {value: [...]}, {items: [...]}, {lists: [...]}
    - {data: {value: [...]}} etc.
    - If a single dict looks like an item, wrap it.
    """
    if isinstance(result, list):
        return result
    if not isinstance(result, dict):
        return []
    # direct lists
    for k in ("value", "items", "lists"):
        v = result.get(k)
        if isinstance(v, list):
            return v
    # nested in data
    data = result.get("data")
    if isinstance(data, dict):
        for k in ("value", "items", "lists"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    # fallback: maybe it's a single item
    if result:
        return [result]
    return []


def _escape_odata(s: str) -> str:
    """Escape single quotes for OData literal strings."""
    return s.replace("'", "''")


def _pst_today_range_iso() -> (str, str):
    """Return ISO-8601 start/end of 'today' in America/Los_Angeles (best effort)."""
    # Prefer zoneinfo if available
    start_iso = end_iso = None
    try:
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        start_iso = start.isoformat()
        end_iso = end.isoformat()
    except Exception:
        # Fallback to local tz
        from datetime import datetime, timedelta

        now = datetime.now().astimezone()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        start_iso = start.isoformat()
        end_iso = end.isoformat()
    return start_iso, end_iso


def _convert_aqs_to_filter(aqs: str) -> Optional[str]:
    """
    AQS -> OData for common patterns:
      - from:"Name"  (multiple allowed; OR/AND handled)
      - received:last24hours
      - received:today
      - received:yesterday
      - (received:today OR received:yesterday) => last 24 hours (PT)
    Returns OData filter string or None if nothing recognized.
    """

    if not isinstance(aqs, str) or not aqs.strip():
        return None
    s = aqs.strip()

    conds: List[str] = []

    def _negate_sender(name: str) -> Optional[str]:
        target = name.strip()
        if not target:
            return None
        if "@" in target and " " not in target:
            return f"from/emailAddress/address ne '{_escape_odata(target)}'"
        parts = [p for p in re.split(r"\s+", target) if p]
        if not parts:
            return None
        if len(parts) == 1:
            return f"not (contains(from/emailAddress/name,'{_escape_odata(parts[0])}'))"
        inner = " and ".join(
            f"contains(from/emailAddress/name,'{_escape_odata(p)}')" for p in parts
        )
        return f"not ({inner})"

    def _strip_negative(pattern: re.Pattern[str], text_value: str) -> str:
        for match in pattern.finditer(text_value):
            candidate = (match.group(1) or "").strip()
            if not candidate:
                continue
            neg_cond = _negate_sender(candidate)
            if neg_cond:
                conds.append(neg_cond)
        return pattern.sub(" ", text_value)

    s = _strip_negative(re.compile(r'\bNOT\s+from:\s*"([^"]+)"', flags=re.I), s)
    s = _strip_negative(re.compile(r'\bNOT\s+from:\s*([^"\s]+)', flags=re.I), s)

    # --- time window handling (PT) ---
    has_last24 = re.search(r"\breceived\s*:\s*last24hours\b", s, flags=re.I) is not None
    has_today = re.search(r"\breceived\s*:\s*today\b", s, flags=re.I) is not None
    has_yday = re.search(r"\breceived\s*:\s*yesterday\b", s, flags=re.I) is not None

    def _pt_now():
        from datetime import datetime

        try:
            from zoneinfo import ZoneInfo

            return datetime.now(ZoneInfo("America/Los_Angeles"))
        except Exception:
            return datetime.now().astimezone()

    def _pt_start_of_day(dt):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # Compute time range
    if has_last24 or (has_today and has_yday):
        # last 24 hours
        from datetime import timedelta

        now = _pt_now()
        start = now - timedelta(hours=24)
        end = now
        conds.append(
            f"receivedDateTime ge {start.isoformat()} and receivedDateTime lt {end.isoformat()}"
        )
    elif has_today:
        from datetime import timedelta

        now = _pt_now()
        start = _pt_start_of_day(now)
        end = start + timedelta(days=1)
        conds.append(
            f"receivedDateTime ge {start.isoformat()} and receivedDateTime lt {end.isoformat()}"
        )
    elif has_yday:
        from datetime import timedelta

        now = _pt_now()
        yday = _pt_start_of_day(now) - timedelta(days=1)
        end = yday + timedelta(days=1)
        conds.append(
            f"receivedDateTime ge {yday.isoformat()} and receivedDateTime lt {end.isoformat()}"
        )

    # --- unread flag handling ---
    for match in re.finditer(r'\bisread\s*:\s*(?:"([^"]+)"|([^\s]+))', s, flags=re.I):
        token = (match.group(1) or match.group(2) or "").strip().lower()
        if token in {"no", "false", "0", "off", "unread"}:
            cond = "isRead eq false"
        elif token in {"yes", "true", "1", "on", "read"}:
            cond = "isRead eq true"
        else:
            continue
        if cond not in conds:
            conds.append(cond)

    # --- sender handling ---
    # Collect all from:"..." tokens
    from_tokens = re.findall(r'from:\s*"([^"]+)"', s, flags=re.I)

    if from_tokens:
        # Detect explicit OR between from:"..." tokens (outside quotes)
        has_or_between_tokens = " OR " in re.sub(r'"[^"]*"', '""', s)

        token_conds: List[str] = []
        for tok in from_tokens:
            name = tok.strip()
            if not name:
                continue
            parts = [p for p in re.split(r"\s+", name) if p]
            if len(parts) == 1:
                token_conds.append(
                    f"contains(from/emailAddress/name,'{_escape_odata(parts[0])}')"
                )
            else:
                and_parts = " and ".join(
                    f"contains(from/emailAddress/name,'{_escape_odata(p)}')"
                    for p in parts
                )
                token_conds.append(f"({and_parts})")

        if token_conds:
            if has_or_between_tokens:
                conds.append("(" + " or ".join(token_conds) + ")")
            else:
                conds.append("(" + " and ".join(token_conds) + ")")

    if not conds:
        return None

    return " and ".join(conds)


class Tools:
    def __init__(self):
        self.base = os.getenv(
            "MS365_MCP_BASE", "http://127.0.0.1:3000"
        )  # override if reverse-proxying
        self.timeout = int(os.getenv("MS365_MCP_TIMEOUT", "60"))

    # ---------- helpers ----------
    def _normalize_args(
        self, tool: str, args: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Normalize arguments WITHOUT breaking case-sensitive param names.

        - Apply ALIAS_KEYS (map source -> canonical) only if canonical not present.
        - For array-like keys (orderby/select/expand), accept several casings on input
          but write using the canonical lowercase key and coerce to list.
        - DO NOT lowercase arbitrary keys (preserve original casing for things like todoTaskListId).
        """
        d_in = dict(args or {})
        d: Dict[str, Any] = {}

        # 1) Copy through while applying aliases (no global lowercasing)
        for k, v in d_in.items():
            if k in ALIAS_KEYS and ALIAS_KEYS[k] not in d_in:
                d[ALIAS_KEYS[k]] = v  # write canonical key
            else:
                d[k] = v

        # 2) Coerce known array keys; accept multiple casings, emit canonical lowercase
        def _pop_any_case(src: Dict[str, Any], *names: str):
            for name in names:
                if name in src:
                    return src.pop(name)
            return None

        ov = _pop_any_case(d, "orderby", "orderBy", "OrderBy", "ORDERBY")
        if ov is not None:
            d["orderby"] = ov if isinstance(ov, list) else [ov]

        sv = _pop_any_case(d, "select", "Select", "SELECT")
        if sv is not None:
            d["select"] = sv if isinstance(sv, list) else [sv]

        ev = _pop_any_case(d, "expand", "Expand", "EXPAND")
        if ev is not None:
            d["expand"] = ev if isinstance(ev, list) else [ev]

        return d

    def _sanitize_select_and_orderby(self, tool: str, args: Dict[str, Any]) -> None:
        """Guard against Graph 400s by trimming unsupported select/orderby."""
        # lists: $orderby frequently invalid
        if tool == "list-todo-task-lists":
            args.pop("orderby", None)

        # orderby: remove nested paths and non-allowed clauses
        if "orderby" in args:
            args["orderby"] = [o for o in args["orderby"] if "/" not in o]
            if tool in SAFE_ORDERBY:
                args["orderby"] = [
                    o for o in args["orderby"] if o in SAFE_ORDERBY[tool]
                ]
            if not args["orderby"]:
                args.pop("orderby", None)

        # select: keep only known-good fields per tool
        if "select" in args and tool in SAFE_SELECTS:
            args["select"] = [s for s in args["select"] if s in SAFE_SELECTS[tool]]
            if not args["select"]:
                args.pop("select", None)

    def _normalize_tool_payload(self, tool: str, args: Dict[str, Any]) -> None:
        """Apply tool-specific coercions so payloads align with MCP schemas."""
        if not isinstance(args, dict):
            return

        if tool in {"create-draft-email", "send-mail"}:
            message_seed = args.pop("body", None)
            if message_seed is None and "message" in args:
                message_seed = args.pop("message")

            message_data: Dict[str, Any] = {}
            if isinstance(message_seed, dict):
                message_data.update(message_seed)
            elif message_seed is not None:
                message_data["body"] = message_seed

            for key in (
                "toRecipients",
                "to",
                "recipients",
                "ccRecipients",
                "cc",
                "bccRecipients",
                "bcc",
                "replyTo",
                "replyToRecipients",
                "subject",
                "importance",
                "sensitivity",
                "attachments",
                "internetMessageHeaders",
                "body",
                "content",
                "contentType",
                "html",
                "text",
            ):
                if key in args:
                    message_data[key] = args.pop(key)

            message = _normalize_message_payload(message_data)
            args["body"] = message

            if tool == "send-mail":
                if "saveToSentItems" in args and isinstance(
                    args["saveToSentItems"], str
                ):
                    args["saveToSentItems"] = args[
                        "saveToSentItems"
                    ].strip().lower() not in {"false", "0", "no", "off"}
                elif "saveToSentItems" not in args:
                    args["saveToSentItems"] = True

        elif tool in {"create-todo-task", "update-todo-task"}:
            payload_seed = args.pop("body", None)
            if isinstance(payload_seed, dict):
                seed: Any = dict(payload_seed)
            else:
                seed = payload_seed

            for key in (
                "title",
                "status",
                "importance",
                "dueDateTime",
                "reminderDateTime",
                "completedDateTime",
                "startDateTime",
                "categories",
                "isReminderOn",
                "body",
                "content",
                "contentType",
                "text",
                "html",
            ):
                if key in args:
                    value = args.pop(key)
                    if isinstance(seed, dict):
                        seed[key] = value
                    elif seed is None:
                        seed = {key: value}
                    else:
                        seed = {"body": seed, key: value}

            patch = _normalize_todo_patch(seed)
            for flag in ("isReminderOn", "completed", "hasAttachments"):
                if flag in patch and isinstance(patch[flag], str):
                    patch[flag] = patch[flag].strip().lower() in {
                        "true",
                        "1",
                        "yes",
                        "y",
                        "on",
                    }
            if "importance" in patch and isinstance(patch["importance"], str):
                patch["importance"] = patch["importance"].lower()
            if "status" in patch and isinstance(patch["status"], str):
                patch["status"] = patch["status"].lower()
            args["body"] = patch

        else:
            if "body" in args and isinstance(args["body"], str):
                args["body"] = _normalize_item_body(args["body"])

    def _rpc(self, access_token: str, method_name: str, params: Dict[str, Any]) -> Any:
        url = f"{self.base}/mcp"
        headers = {
            "Authorization": f"Bearer {access_token}",  # token not logged
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",  # required by this HTTP transport
            "mcp-protocol-version": "2024-11-05",  # harmless if not required
        }
        body = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "tools/call",
            "params": {"name": method_name, "arguments": params},
        }
        _safelog(
            "REQUEST.headers",
            {
                k: ("<redacted>" if k.lower() == "authorization" else v)
                for k, v in headers.items()
            },
        )
        _safelog("REQUEST.body", body)
        return requests.post(url, json=body, headers=headers, timeout=self.timeout)

    def _rpc_json(self, resp: Any) -> Dict[str, Any]:
        try:
            return resp.json()
        except Exception:
            return {"raw": getattr(resp, "text", "")}

    def _looks_like_schema_error(self, data: Dict[str, Any]) -> bool:
        try:
            err = data.get("error", {})
            code = err.get("code")
            msg = (err.get("message") or "").lower()
            return (
                code in (-32602,) or "invalid argument" in msg or "invalid_type" in msg
            )
        except Exception:
            return False

    def _resolve_mail_folder_id(self, access_token: str, name: str) -> Optional[str]:
        """Resolve an Outlook folder displayName (e.g., 'Inbox') to its id."""
        resp = self._rpc(
            access_token,
            "list-mail-folders",
            {
                "filter": f"displayName eq '{_escape_odata(name)}'",
                "select": ["id", "displayName"],
            },
        )

        data = self._rpc_json(resp)
        if isinstance(data, dict) and "error" in data and "result" not in data:
            return None

        result_payload = _unwrap_streaming_result(data.get("result", data))
        items = _extract_items_flex(result_payload)
        target = str(name).strip().lower()
        for item in items:
            display = str(item.get("displayName", "")).strip().lower()
            if display == target:
                return item.get("id")

        try:
            return items[0].get("id")
        except Exception:
            return None

    def _resolve_todo_list_id(self, access_token: str, name: str) -> Optional[str]:
        """Resolve a To Do list displayName (e.g., 'Tasks') to its id; fallback to well-known default list."""
        resp = self._rpc(
            access_token,
            "list-todo-task-lists",
            {"select": ["id", "displayName", "wellknownListName"]},
        )
        data = self._rpc_json(resp)
        result = data.get("result", data) if isinstance(data, dict) else data
        items = _extract_items_flex(result)

        # 1) exact displayName match
        for x in items:
            if x.get("displayName") == name:
                return x.get("id")
        # 2) case-insensitive
        target = str(name).strip().lower()
        for x in items:
            if str(x.get("displayName", "")).strip().lower() == target:
                return x.get("id")
        # 3) well-known fallback
        if target in ("tasks", "task", "default", "default list", "defaultlist"):
            for x in items:
                if str(x.get("wellknownListName", "")).lower() == "defaultlist":
                    return x.get("id")
        if target in ("flagged emails", "flagged", "flaggedemails"):
            for x in items:
                if str(x.get("wellknownListName", "")).lower() == "flaggedemails":
                    return x.get("id")
        # 4) last resort: use defaultList if present
        for x in items:
            if str(x.get("wellknownListName", "")).lower() == "defaultlist":
                return x.get("id")
        return None

    # ---------- main entry ----------
    def ms365_mcp_call(
        self,
        tool: str,
        payload: Optional[Dict[str, Any]] = None,
        __oauth_token__: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Require per-user Microsoft access token from Open WebUI SSO
        if not __oauth_token__ or "access_token" not in __oauth_token__:
            return {"error": "No Microsoft access token from SSO."}

        # Guard against arbitrary endpoints
        if tool not in ALLOWED_TOOLS:
            return {
                "error": f"Tool '{tool}' is not allowed.",
                "allowed": sorted(ALLOWED_TOOLS),
            }

        access_token = __oauth_token__["access_token"]

        # Normalize args (global) â€” preserves casing for case-sensitive keys
        args = self._normalize_args(tool, payload)

        # Convenience: "folder": "Inbox" allowed for both mail listing tools
        if tool in ("list-mail-folder-messages", "list-mail-messages"):
            if "mailFolderId" not in args and ("folder" in args or "Folder" in args):
                folder = args.pop("folder", None) or args.pop("Folder", None)
                if folder:
                    folder_id = self._resolve_mail_folder_id(access_token, folder)
                    if not folder_id:
                        return {
                            "error": f"Mail folder '{folder}' not found or not accessible"
                        }
                    args["mailFolderId"] = folder_id

        # AQS -> OData conversion for Mail searches (handles from:"..." and received:today)
        if (
            tool in ("list-mail-messages", "list-mail-folder-messages")
            and "search" in args
        ):
            search_value = args.get("search")
            if isinstance(search_value, str):
                search_value = _normalize_exclusion_phrases(search_value)
                cleaned_search, folder_name = _pull_folder_from_aqs(search_value)
                if (
                    folder_name
                    and "mailFolderId" not in args
                    and "folder" not in args
                    and "Folder" not in args
                ):
                    args["folder"] = folder_name
                if cleaned_search:
                    args["search"] = cleaned_search
                else:
                    args.pop("search", None)

            raw_aqs = args.get("search") or ""
            aqs = _normalize_exclusion_phrases(raw_aqs)
            if aqs and aqs != raw_aqs:
                args["search"] = aqs
            elif not aqs:
                args.pop("search", None)
            if isinstance(aqs, str) and (
                re.search(r'from:\s*"', aqs, flags=re.I)
                or re.search(r"\breceived\s*:\s*today\b", aqs, flags=re.I)
                or re.search(r"\bisread\s*:\s*", aqs, flags=re.I)
                or re.search(r"\bNOT\s+from:\s*", aqs, flags=re.I)
            ):
                filt = _convert_aqs_to_filter(aqs)
                if filt:
                    # Replace 'search' with 'filter'; Graph disallows using both together
                    args.pop("search", None)
                    if "filter" in args:
                        args["filter"] = f"({args['filter']}) and ({filt})"
                    else:
                        args["filter"] = filt

        if tool in {
            "create-todo-task",
            "update-todo-task",
            "delete-todo-task",
            "get-todo-task",
        }:
            if "todoTaskListId" not in args:
                list_name = (
                    args.pop("taskList", None)
                    or args.pop("tasklist", None)
                    or args.pop("taskListName", None)
                    or args.pop("tasklistname", None)
                    or args.pop("list", None)
                    or args.pop("List", None)
                    or "Tasks"
                )
                tl_id = self._resolve_todo_list_id(access_token, list_name)
                if not tl_id:
                    if DEBUG:
                        _safelog("TODO.resolve.failed", {"asked_for": list_name})
                    return {
                        "error": f"To Do list '{list_name}' not found or not accessible"
                    }
                args["todoTaskListId"] = tl_id

            if (
                tool in {"update-todo-task", "delete-todo-task", "get-todo-task"}
                and "todoTaskId" not in args
            ):
                for key in ("taskId", "TaskId", "TaskID", "taskid", "id"):
                    if key in args:
                        args["todoTaskId"] = args.pop(key)
                        break

        # Convenience: for list-todo-tasks, accept aliases and names; set defaults and sanitize
        if tool == "list-todo-tasks":
            # Map accidental lowercase to canonical (if some prior prompt produced it)
            if "todoTaskListId" not in args and "todotasklistid" in args:
                args["todoTaskListId"] = args.pop("todotasklistid")

            # Map alias if present
            if "todoTaskListId" not in args:
                if "taskListId" in args:
                    args["todoTaskListId"] = args.pop("taskListId")
                elif "tasklistid" in args:
                    args["todoTaskListId"] = args.pop("tasklistid")

            # Resolve by list name if still missing
            if "todoTaskListId" not in args:
                list_name = (
                    args.pop("taskList", None)
                    or args.pop("tasklist", None)
                    or args.pop("taskListName", None)
                    or args.pop("tasklistname", None)
                    or args.pop("list", None)
                    or args.pop("List", None)
                    or "Tasks"
                )
                tl_id = self._resolve_todo_list_id(access_token, list_name)
                if not tl_id:
                    if DEBUG:
                        _safelog("TODO.resolve.failed", {"asked_for": list_name})
                    return {
                        "error": f"To Do list '{list_name}' not found or not accessible"
                    }
                args["todoTaskListId"] = tl_id

            # Helpful default order (simple field, avoids nested path rejections)
            args.setdefault("orderby", ["createdDateTime desc"])

        self._normalize_tool_payload(tool, args)

        # Final safety: trim unsupported select/orderby for this tool
        self._sanitize_select_and_orderby(tool, args)

        # First attempt
        t0 = time.monotonic()
        resp = self._rpc(access_token, tool, args)
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        data = self._rpc_json(resp)
        _safelog(
            "RESPONSE.json",
            {"status": getattr(resp, "status_code", None), "body": data},
        )

        # Success path (JSON-RPC result or plain 2xx JSON)
        status_code = getattr(resp, "status_code", 500)
        if status_code < 400 and isinstance(data, dict) and "error" not in data:
            out = data.get("result", data)
            if isinstance(out, dict) and "elapsed_ms" not in out:
                out["elapsed_ms"] = elapsed_ms
            return out

        # Auto-retry once on schema/shape errors (-32602 etc.)
        if isinstance(data, dict) and self._looks_like_schema_error(data):
            # Try to coerce fields mentioned in error details into arrays
            coerced = dict(args)
            err = data.get("error", {})
            details = err.get("data") or err.get("details") or err.get("meta")
            if isinstance(details, list):
                for item in details:
                    if not isinstance(item, dict):
                        continue
                    path = item.get("path")
                    expected = item.get("expected")
                    if isinstance(path, list) and path:
                        k = str(path[0])
                        if (
                            expected == "array"
                            and k in coerced
                            and isinstance(coerced[k], str)
                        ):
                            coerced[k] = [coerced[k]]
            self._normalize_tool_payload(tool, coerced)
            _safelog("RETRY.coercedArgs", coerced)
            resp2 = self._rpc(access_token, tool, coerced)
            data2 = self._rpc_json(resp2)
            _safelog(
                "RETRY.response",
                {"status": getattr(resp2, "status_code", None), "body": data2},
            )
            status_code2 = getattr(resp2, "status_code", 500)
            if status_code2 < 400 and isinstance(data2, dict) and "error" not in data2:
                out = data2.get("result", data2)
                if isinstance(out, dict) and "elapsed_ms" not in out:
                    out["elapsed_ms"] = elapsed_ms
                return out
            return {"status": status_code2, "error": data2}

        # Otherwise return first error with timing
        return {"status": status_code, "error": data, "elapsed_ms": elapsed_ms}
