use clap::Args;
use eyre::Report;
use serde_json::{json, Value};
use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::{FidRequest, Message, UserDataRequest};
use crate::proto::hub_service_client::HubServiceClient;

#[derive(Args, Debug)]
pub struct FidCommand {
    #[clap(flatten)]
    base: BaseRpcConfig,

    #[arg(long)]
    fid: u64,

    #[arg(long, short = 't')]
    user_data_type: Option<i32>,
}

impl FidCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = self.base.load_endpoint()?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();

        if let Some(t) = self.user_data_type {
            let response = client
                .get_user_data(UserDataRequest{fid: self.fid, user_data_type: t})
                .await
                .map_err(|e| Report::new(e))?;
            let str_response = serde_json::to_string_pretty(&response.into_inner()).map_err(|e| Report::new(e))?;
            return Ok(println!("{}", str_response))
        }

        let response = client
            .get_user_data_by_fid(FidRequest{fid: self.fid, page_size: None, page_token: None, reverse: None})
            .await
            .map_err(|e| Report::new(e))?;

        let str_response = pretty_print_messages(&response.into_inner().messages);

        Ok(println!("{}", str_response))
    }
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

fn bytes_to_hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    hex.push_str("0x");
    for &byte in bytes {
        hex.push(HEX_CHARS[(byte >> 4) as usize] as char);
        hex.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    hex
}

fn pretty_print_messages(messages: &Vec<Message>) -> String {
    let json_messages: Vec<Value> = messages.iter().map(|msg: &Message| {
        let message_bytes: Vec<u8> = prost::Message::encode_to_vec(msg);
        json!({
            "data": msg.data,
            "hash": bytes_to_hex_string(&msg.hash),
            "hash_scheme": msg.hash_scheme,
            "signature": bytes_to_hex_string(&msg.signature),
            "signature_scheme": msg.signature_scheme,
            "signer": bytes_to_hex_string(&msg.signer),
            "message_bytes": format!("{:?}", message_bytes),
            // data_bytes is omitted
        })
    }).collect();

    serde_json::to_string_pretty(&json_messages).unwrap_or_else(|_| "[]".to_string())
}

