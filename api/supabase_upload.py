import os
from typing import List, Dict, Any, Optional

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("[supabase_upload] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY; skipping upload.")
        return None
    try:
        from supabase import create_client, Client  # type: ignore
        _client = create_client(url, key)
        return _client
    except Exception as e:
        print(f"[supabase_upload] Failed to create client: {e}")
        return None


def upload_csv_and_record(
    *,
    user_id: Optional[str],
    job_id: str,
    csv_bytes: bytes,
    filename: str,
    asins: List[str],
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Uploads the CSV to Supabase Storage and inserts a row in recent_reports.
    Returns the storage path if successful, else None.
    """
    client = _get_client()
    if client is None:
        return None

    bucket = os.getenv("SUPABASE_BUCKET_REPORTS", "reports")
    path = f"{user_id or 'anonymous'}/{job_id}/{filename}"

    # Upload to Storage (upsert true to avoid collisions if retried)
    try:
        # Try official SDK first
        client.storage.from_(bucket).upload(path=path, file=csv_bytes)
    except Exception as e:
        print(f"[supabase_upload] SDK upload exception: {e}. Trying raw HTTP...")
        # Fallback to raw HTTP to avoid env-specific httpx header issues
        try:
            import requests
            url = (os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")).rstrip("/")
            key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            endpoint = f"{url}/storage/v1/object/{bucket}/{path}"
            headers = {"Authorization": f"Bearer {key}", "apikey": key}
            files = {"file": (filename, csv_bytes, "text/csv")}
            resp = requests.post(endpoint, headers=headers, files=files)
            if not resp.ok:
                print(f"[supabase_upload] Raw HTTP upload failed: {resp.status_code} {resp.text}")
                return None
        except Exception as e2:
            import traceback
            print(f"[supabase_upload] Upload failed: {e} | Fallback error: {e2}")
            traceback.print_exc()
            return None

    # Ensure profile row exists (FK) then insert metadata row (best-effort)
    try:
        print(f"[supabase_upload] insert recent_reports user_id={user_id} job_id={job_id} path={path}")
        if user_id:
            user_email = None
            try:
                user_email = (meta or {}).get("user_email")
            except Exception:
                user_email = None
            # upsert profile to satisfy FK
            client.table("profiles").upsert({
                "id": user_id,
                "email": user_email,
            }).execute()

        meta_payload = dict(meta or {})
        # include list of asins in meta for frontend TXT links
        if asins:
            meta_payload["asins"] = asins
        data = {
            "user_id": user_id,
            "job_id": job_id,
            "asin": ",".join(asins[:5]) + ("+" + str(len(asins) - 5) if len(asins) > 5 else ""),
            "title": f"Batch report ({len(asins)} ASIN)",
            "summary": None,
            "report_url": path,
            "format": "csv",
            "meta": meta_payload,
        }
        client.table("recent_reports").insert(data).execute()
        print(f"[supabase_upload] inserted recent_reports: {data['id'] if 'id' in data else 'ok'}")
    except Exception as e:
        # Ignore DB errors to not break the job, but log
        print(f"[supabase_upload] DB insert failed: {e}")

    return path


def upload_file(
    *,
    user_id: Optional[str],
    job_id: str,
    file_bytes: bytes,
    filename: str,
    content_type: str = "application/octet-stream",
    bucket_env: str = "SUPABASE_BUCKET_REPORTS",
) -> Optional[str]:
    """Uploads arbitrary bytes to Supabase Storage under user/job folder.
    Returns the storage path if successful, else None.
    """
    client = _get_client()
    if client is None:
        return None

    bucket = os.getenv(bucket_env, "reports")
    path = f"{user_id or 'anonymous'}/{job_id}/{filename}"

    # Try SDK, then fallback to raw HTTP
    try:
        client.storage.from_(bucket).upload(path=path, file=file_bytes)
        return path
    except Exception:
        try:
            import requests
            url = (os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")).rstrip("/")
            key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            endpoint = f"{url}/storage/v1/object/{bucket}/{path}"
            headers = {"Authorization": f"Bearer {key}", "apikey": key}
            files = {"file": (filename, file_bytes, content_type)}
            resp = requests.post(endpoint, headers=headers, files=files)
            if resp.ok:
                return path
            else:
                print(f"[supabase_upload] Raw HTTP upload failed: {resp.status_code} {resp.text}")
                return None
        except Exception as e2:
            import traceback
            print(f"[supabase_upload] upload_file fallback error: {e2}")
            traceback.print_exc()
            return None
