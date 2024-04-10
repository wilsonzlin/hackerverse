use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;
use itertools::Itertools;
use std::fs::File;
use std::sync::Arc;

const FLUSH_THRESHOLD: usize = 100_000;

pub struct ArrowIpcOutput<R> {
  buf: Vec<R>,
  schema: Schema,
  to_columnar: fn(Vec<R>) -> Vec<ArrayRef>,
  writer: FileWriter<File>,
}

impl<R> ArrowIpcOutput<R> {
  pub fn new(name: &str, schema: Schema, to_columnar: fn(Vec<R>) -> Vec<ArrayRef>) -> Self {
    let out = File::create(format!("/hndr-data/{name}.arrow")).unwrap();
    let writer = FileWriter::try_new(out, &schema).unwrap();
    Self {
      buf: Vec::new(),
      schema,
      to_columnar,
      writer,
    }
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
  }
}
