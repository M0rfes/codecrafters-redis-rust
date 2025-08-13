mod reader;
mod data_store;
pub mod command;

use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::info;

pub async fn handle_connection(stream: TcpStream)->Result<(),Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr().unwrap();
    info!("Accepted connection from {}", peer_addr);

    // Wrap the TCP stream with a RESP codec.
    let framed = Framed::new(stream, reader::RespCodec);
    // Split into writer (sink) and reader (stream) halves.
    let ( writer_sink, reader_stream) = framed.split();

    // Create an mpsc channel to pass serialized responses from the reader to the writer.
    let (tx, rx) = mpsc::channel::<command::Response>(32);
    // let (command_tx, command_rx) = mpsc::channel::<common::message::Command>(32);

    let data_store = data_store::DataStore::new(rx, writer_sink);
    let data_store_handle = tokio::spawn(async move {data_store.run().await.unwrap()});
    
    let reader = reader::Reader::new(reader_stream, tx);
    reader.run().await?;
    data_store_handle.await?;

    info!("Connection with {} closed", peer_addr);
    Ok(())
}