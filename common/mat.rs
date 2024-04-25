use itertools::Itertools;
use std::io::Read;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

pub trait Elem {
  const BYTES_PER_ELEM: usize;

  fn from_le_bytes(raw: &[u8]) -> Self;
}

impl Elem for f32 {
  const BYTES_PER_ELEM: usize = 4;

  fn from_le_bytes(raw: &[u8]) -> Self {
    f32::from_le_bytes(raw.try_into().unwrap())
  }
}

impl Elem for u32 {
  const BYTES_PER_ELEM: usize = 4;

  fn from_le_bytes(raw: &[u8]) -> Self {
    u32::from_le_bytes(raw.try_into().unwrap())
  }
}

pub fn load_mat<E: Copy + Default + Elem>(name: &str, rows: usize, cols: usize) -> Vec<Vec<E>> {
  let raw_row_len = cols * E::BYTES_PER_ELEM;
  let raw_len = rows * raw_row_len;
  let mut f = std::fs::File::open(format!("/hndr-data/{name}.mmap")).unwrap();
  let mut raw = vec![0u8; raw_len];
  let n = f.read_to_end(&mut raw).unwrap();
  assert_eq!(n, raw_len);
  let mut out = (0..rows).map(|_| vec![E::default(); cols]).collect_vec();
  for i in 0..rows {
    for j in 0..cols {
      let offset = raw_row_len * i + E::BYTES_PER_ELEM * j;
      out[i][j] = E::from_le_bytes(&raw[offset..offset + E::BYTES_PER_ELEM]);
    }
  }
  out
}

pub struct MatrixFile {
  count_file_name: String,
  temp_file_name: String,
  dest_file_name: String,
  out_id: BufWriter<File>,
  out_data: BufWriter<File>,
  count: usize,
}

impl MatrixFile {
  pub async fn new(name: &str) -> Option<Self> {
    let dest_file_name = format!("/hndr-data/mat_{}_data.mmap", name);
    if Path::new(&dest_file_name).exists() {
      println!("{name} already exists, skipping");
      return None;
    };
    let temp_file_name = format!("{dest_file_name}.tmp");
    const WRITE_BUF_SIZE: usize = 1024 * 1024 * 1024 * 128;
    let out_data =
      BufWriter::with_capacity(WRITE_BUF_SIZE, File::create(&temp_file_name).await.unwrap());
    let out_id = BufWriter::with_capacity(
      WRITE_BUF_SIZE,
      File::create(format!("/hndr-data/mat_{}_ids.mmap", name))
        .await
        .unwrap(),
    );
    Some(Self {
      count_file_name: format!("/hndr-data/mat_{}_count.txt", name),
      temp_file_name,
      dest_file_name,
      out_id,
      out_data,
      count: 0,
    })
  }

  pub async fn push(&mut self, id: u32, data_raw: &[u8]) {
    self.count += 1;
    self.out_id.write_u32_le(id).await.unwrap();
    self.out_data.write_all(data_raw).await.unwrap();
  }

  pub async fn finish(&mut self) {
    fs::write(&self.count_file_name, self.count.to_string())
      .await
      .unwrap();
    self.out_data.flush().await.unwrap();
    self.out_id.flush().await.unwrap();
    fs::rename(&self.temp_file_name, &self.dest_file_name)
      .await
      .unwrap();
  }
}
