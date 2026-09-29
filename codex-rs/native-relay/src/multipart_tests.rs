use super::*;
use pretty_assertions::assert_eq;

#[test]
fn parses_file_and_purpose_parts() {
    let body = Bytes::from_static(
        b"--xyz\r\n\
Content-Disposition: form-data; name=\"purpose\"\r\n\r\n\
assistants\r\n\
--xyz\r\n\
Content-Disposition: form-data; name=\"file\"; filename=\"notes.txt\"\r\n\
Content-Type: text/plain\r\n\r\n\
line one\r\nline two\r\n\
--xyz--\r\n",
    );

    let form = parse_file_upload("multipart/form-data; boundary=\"xyz\"", &body);

    assert_eq!(
        form,
        Ok(FileUploadForm {
            file_name: "notes.txt".to_string(),
            contents: Bytes::from_static(b"line one\r\nline two"),
            purpose: Some("assistants".to_string()),
        })
    );
}

#[test]
fn rejects_bodies_without_a_file_part_or_boundary() {
    let without_file = Bytes::from_static(
        b"--b\r\nContent-Disposition: form-data; name=\"purpose\"\r\n\r\nx\r\n--b--",
    );
    assert_eq!(
        [
            parse_file_upload("multipart/form-data; boundary=b", &without_file),
            parse_file_upload("application/json", &without_file),
        ],
        [
            Err("multipart body has no `file` part".to_string()),
            Err("expected a multipart/form-data body with a boundary".to_string()),
        ]
    );
}
