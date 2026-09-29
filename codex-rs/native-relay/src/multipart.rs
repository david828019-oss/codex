//! Minimal `multipart/form-data` reader for the OpenAI-style `POST /v1/files` upload.
//!
//! The relay buffers request bodies (bounded by `--max-body-bytes`), so parts are sliced out of
//! the buffered body without copying.

use bytes::Bytes;

/// The parts of an OpenAI `POST /v1/files` form the relay uses.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FileUploadForm {
    pub(crate) file_name: String,
    pub(crate) contents: Bytes,
    pub(crate) purpose: Option<String>,
}

pub(crate) fn parse_file_upload(
    content_type: &str,
    body: &Bytes,
) -> Result<FileUploadForm, String> {
    let boundary = boundary(content_type)
        .ok_or_else(|| "expected a multipart/form-data body with a boundary".to_string())?;
    let delimiter = format!("--{boundary}").into_bytes();
    let next_delimiter = format!("\r\n--{boundary}").into_bytes();

    let mut position = find(body, &delimiter, 0)
        .ok_or_else(|| "multipart body has no opening boundary".to_string())?
        + delimiter.len();
    let mut file = None;
    let mut purpose = None;
    loop {
        let rest = &body[position..];
        if rest.starts_with(b"--") {
            break;
        }
        if !rest.starts_with(b"\r\n") {
            return Err("malformed multipart boundary line".to_string());
        }
        let headers_start = position + 2;
        let headers_end = find(body, b"\r\n\r\n", headers_start)
            .ok_or_else(|| "multipart part is missing its header terminator".to_string())?;
        let content_start = headers_end + 4;
        let content_end = find(body, &next_delimiter, content_start)
            .ok_or_else(|| "multipart body has no closing boundary".to_string())?;
        let headers = std::str::from_utf8(&body[headers_start..headers_end])
            .map_err(|_| "multipart part headers are not UTF-8".to_string())?;
        let disposition = headers
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-disposition"))
            .map(|(_, value)| value);
        if let Some(disposition) = disposition {
            match disposition_param(disposition, "name").as_deref() {
                Some("file") => {
                    file = Some((
                        disposition_param(disposition, "filename")
                            .filter(|name| !name.is_empty())
                            .unwrap_or_else(|| "file".to_string()),
                        body.slice(content_start..content_end),
                    ));
                }
                Some("purpose") => {
                    purpose = std::str::from_utf8(&body[content_start..content_end])
                        .ok()
                        .map(|value| value.trim().to_string());
                }
                _ => {}
            }
        }
        position = content_end + next_delimiter.len();
    }

    let (file_name, contents) =
        file.ok_or_else(|| "multipart body has no `file` part".to_string())?;
    Ok(FileUploadForm {
        file_name,
        contents,
        purpose,
    })
}

fn boundary(content_type: &str) -> Option<String> {
    let mut params = content_type.split(';');
    let mime = params.next()?.trim();
    if !mime.eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    params
        .filter_map(|param| param.split_once('='))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("boundary"))
        .map(|(_, value)| value.trim().trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
}

fn disposition_param(disposition: &str, name: &str) -> Option<String> {
    disposition
        .split(';')
        .filter_map(|param| param.split_once('='))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim().trim_matches('"').to_string())
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| from + offset)
}

#[cfg(test)]
#[path = "multipart_tests.rs"]
mod tests;
