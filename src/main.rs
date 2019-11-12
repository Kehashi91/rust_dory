use std::mem::size_of_val;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_MESSAGE_SIZE: i32 = 2_147_483_647; // 2 ** 31 - 1
const MAX_TOPIC_SIZE: i32 = 32_767; // 2 ** 15 - 1

#[derive(Debug)]
struct DoryMessageAnyKey<'a> {
    message_size: i32,
    api_key: i16,
    api_key_version: i16,
    flags: i16,
    timestamp: i64,
    topic_lenght: i16,
    key_length: i32,
    value_length: i32,
    topic_value: &'a [u8],
    key_value: &'a [u8],
    value: &'a [u8],
}

impl<'a> DoryMessageAnyKey<'a> {
    fn new(
        topic_value: &'a [u8],
        key_value: &'a [u8],
        value: &'a [u8],
    ) -> Result<DoryMessageAnyKey<'a>, &'static str> {
        let mut msg = DoryMessageAnyKey {
            message_size: 0,
            api_key: 256,
            api_key_version: 0,
            flags: 0,
            topic_lenght: topic_value.len() as i16,
            timestamp: DoryMessageAnyKey::get_time_miliseconds(),
            key_length: key_value.len() as i32,
            value_length: value.len() as i32,
            topic_value,
            key_value,
            value,
        };
        msg.get_size();

        match msg.check_size() {
            Ok(_) => Ok(msg),
            Err(e) => Err(e),
        }
    }

    fn get_time_miliseconds() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn get_size(&mut self) {
        let message_size = size_of_val(&self.message_size)
            + size_of_val(&self.api_key)
            + size_of_val(&self.api_key_version)
            + size_of_val(&self.flags)
            + size_of_val(&self.topic_lenght)
            + size_of_val(&self.timestamp)
            + size_of_val(&self.key_length)
            + size_of_val(&self.value_length)
            + self.topic_value.len()
            + self.key_value.len()
            + self.value.len();

        self.message_size = message_size as i32;
    }

    fn check_size(&self) -> Result<(), &'static str> {
        if self.message_size > MAX_MESSAGE_SIZE {
            return Err("Message too large");
        } else if self.topic_lenght > MAX_TOPIC_SIZE as i16 {
            return Err("Topic too large");
        };
        Ok(())
    }

    // Ideally would use bincode, but its a diffrent format than dory requires
    fn serialize(&self) -> Vec<u8> {
        let mut buff: Vec<u8> = Vec::new();

        buff.extend_from_slice(&self.message_size.to_be_bytes()); // append for time being to get right size
        buff.extend_from_slice(&self.api_key.to_be_bytes());
        buff.extend_from_slice(&self.api_key_version.to_be_bytes());
        buff.extend_from_slice(&self.flags.to_be_bytes());
        buff.extend_from_slice(&self.topic_lenght.to_be_bytes());
        buff.extend_from_slice(&self.topic_value);
        buff.extend_from_slice(&self.timestamp.to_be_bytes());
        buff.extend_from_slice(&self.key_length.to_be_bytes());
        buff.extend_from_slice(&self.key_value);
        buff.extend_from_slice(&self.value_length.to_be_bytes());
        buff.extend_from_slice(&self.value);

        buff
    }
}

fn main() {
    let x = DoryMessageAnyKey::new(b"topic", b"none", b"zorcz");
    println!("{:#?}", x);

    let enc = x.unwrap();
    //let enc2: Vec<u8> = x.serialize();
    println!("{:?}", enc.serialize());
    println!("{:?}", enc.value);
}
