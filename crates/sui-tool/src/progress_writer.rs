use std::fs::{File, OpenOptions};
use anyhow::Context;
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::{SinkExt, StreamExt};
use std::io::{BufRead, BufReader, Write};

#[derive(Clone)]
pub(crate) struct ProgressWriter {
    path:String,
    tx:UnboundedSender<String>
}

impl ProgressWriter {
    pub fn new(path:String) -> Self {
        let (tx, mut rx) = unbounded::<String>();
        let path2= path.clone();
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path2.clone())
            .expect(format!("Failed to open progress file:{}",&path2).as_str());
        tokio::spawn(async move {
            while let Some(d)=rx.next().await{
                writeln!(file, "{}", d).expect("Failed to write progress");
            }
        });
        Self {tx,path}
    }
     pub async fn write(&mut self, data:String)->anyhow::Result<()>{
        self.tx.send(data).await.context("Failed to write progress")
    }
    pub fn get_all(&self)->anyhow::Result<Vec<String>>{
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let lines: Vec<String> = reader
            .lines()
            .collect::<Result<_, _>>()?;
        Ok(lines)
    }
}