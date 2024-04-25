use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use itertools::Itertools;
use std::fs::rename;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub fn load_arrow(name: &str) -> FileReader<File> {
  let f = File::open(format!("/hndr-data/{name}.arrow")).unwrap();
  FileReader::try_new(f, None).unwrap()
}

const FLUSH_THRESHOLD: usize = 100_000;

pub struct ArrowIpcOutput<R> {
  temp_file_name: String,
  dest_file_name: String,
  buf: Vec<R>,
  schema: Schema,
  to_columnar: fn(Vec<R>) -> Vec<ArrayRef>,
  writer: FileWriter<File>,
}

impl<R> ArrowIpcOutput<R> {
  pub fn new(name: &str, schema: Schema, to_columnar: fn(Vec<R>) -> Vec<ArrayRef>) -> Option<Self> {
    let dest_file_name = format!("/hndr-data/{}.arrow", name);
    if Path::new(&dest_file_name).exists() {
      println!("{name} already exists, skipping");
      return None;
    };
    let temp_file_name = format!("{dest_file_name}.tmp");
    let out = File::create(&temp_file_name).unwrap();
    let writer = FileWriter::try_new(out, &schema).unwrap();
    Some(Self {
      temp_file_name,
      dest_file_name,
      buf: Vec::new(),
      schema,
      to_columnar,
      writer,
    })
  }

  fn flush(&mut self) {
    let data = (self.to_columnar)(self.buf.drain(..).collect_vec());
    let batch = RecordBatch::try_new(Arc::new(self.schema.clone()), data).unwrap();
    self.writer.write(&batch).unwrap();
  }

  pub fn push(&mut self, row: R) {
    self.buf.push(row);
    if self.buf.len() >= FLUSH_THRESHOLD {
      self.flush();
    };
  }

  pub fn finish(mut self) {
    if !self.buf.is_empty() {
      self.flush();
    };
    self.writer.finish().unwrap();
    rename(&self.temp_file_name, &self.dest_file_name).unwrap();
  }
}
