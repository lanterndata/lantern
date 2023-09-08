use pgrx::prelude::*;

use flate2::read::GzDecoder;
use ftp::FtpStream;
use tar::Archive;

pgrx::pg_module_magic!();
pub mod dotvecs;
pub mod encoder;

#[macro_use]
extern crate lazy_static;

#[pg_extern(immutable)]
fn clip_text<'a>(text: &'a str) -> Vec<f32> {
    return encoder::clip::process_text(text.to_owned());
}

#[pg_extern(immutable)]
fn clip_image<'a>(path_or_url: &'a str) -> Vec<f32> {
    return encoder::clip::process_image(path_or_url.to_owned());
}

#[pg_extern]
fn get_vectors<'a>(gzippath: &'a str) -> String {
    let url = url::Url::parse(gzippath).unwrap();
    if url.scheme() == "ftp" {
        match download_gzipped_ftp(url) {
            Ok(data) => {
                return data
                    .map(|b| b.unwrap().to_string())
                    .take(10)
                    .collect::<Vec<String>>()
                    .join(" ");
            }
            Err(e) => {
                return e.to_string();
            }
        }
    }
    return "not supported".to_string();
}

fn download_gzipped_ftp(
    url: url::Url,
) -> Result<impl Iterator<Item = Result<u8, std::io::Error>>, Box<dyn std::error::Error>> {
    use std::io::prelude::*;
    assert!(url.scheme() == "ftp");
    let domain = url.host_str().expect("no host");
    let port = url.port().unwrap_or(21);
    let pathurl = url.join("./")?;
    let path = pathurl.path();
    let filename = url
        .path_segments()
        .expect("expected path segments in an ftp url")
        .last()
        .unwrap();

    let mut ftp_stream = FtpStream::connect(format!("{}:{}", domain, port))?;
    ftp_stream
        .login("anonymous", "anonymous")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string()))?;
    ftp_stream.cwd(path)?;
    let file = ftp_stream.get(filename)?;

    let dd = GzDecoder::new(file);
    if false {
        return Ok(dd.bytes());
    }
    let mut a = Archive::new(dd);
    // a.unpack("/tmp/rustftp")?;
    a.entries()
        .unwrap()
        .map(|entry| match entry {
            Ok(e) => {
                let s = String::new();
                notice!("entry name {}", e.path().unwrap().display());
                Ok(s)
            }
            Err(e) => Err(e),
        })
        .for_each(|e| match e {
            Ok(s) => {
                notice!("entry: {}", s);
            }
            Err(e) => {
                notice!("entry: {}", e);
            }
        });
    return Err("not implemented".into());
}

// fn read_file_stream(pathlike: String) -> std::io::Result<std::io::Bytes<dyn std::io::Read>>{
//     use std::io::prelude::*;

//     let res = get(gzippath);
//     if res.is_err() {
//         return res.err().unwrap().to_string();
//     }
//     let resp = res.unwrap();
//     let mut d = GzDecoder::new(resp);

// }

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    // use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_lanterndb_extras() {
        assert_eq!(
            "Hello, lanterndb_extras",
            crate::get_vectors("invalid path")
        );
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
