use std::mem::size_of_val;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_MESSAGE_SIZE: usize = 2_147_483_647;
const MAX_TOPIC_SIZE: usize = 32_767;

trait DoryMessage {
    fn get_time_miliseconds() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
    fn check_size(value: &[u8], topic: &[u8]) -> Result<(), &'static str> {
        // We use separatly calculated lenghts as larger integers to avoid integers overflow
        if value.len() > MAX_MESSAGE_SIZE {
            return Err("Message too large");
        } else if topic.len() > MAX_TOPIC_SIZE {
            return Err("Topic too large");
        };
        Ok(())
    }
}

#[derive(Debug)]
pub struct DoryMessageAnyKey<'a> {
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

impl DoryMessage for DoryMessageAnyKey<'_> {}

impl<'a> DoryMessageAnyKey<'a> {
    pub fn new(
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

        match DoryMessageAnyKey::check_size(msg.value, msg.topic_value) {
            Ok(_) => Ok(msg),
            Err(e) => Err(e),
        }
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

    // Ideally would use bincode, but its a diffrent format than dory requires
    pub fn serialize(&self) -> Vec<u8> {
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

#[derive(Debug)]
pub struct DoryMessageWithKey<'a> {
    message_size: i32,
    api_key: i16,
    api_key_version: i16,
    flags: i16,
    partition_key: i32,
    timestamp: i64,
    topic_lenght: i16,
    key_length: i32,
    value_length: i32,
    topic_value: &'a [u8],
    key_value: &'a [u8],
    value: &'a [u8],
}

impl DoryMessage for DoryMessageWithKey<'_> {}

impl<'a> DoryMessageWithKey<'a> {
    pub fn new(
        topic_value: &'a [u8],
        key_value: &'a [u8],
        value: &'a [u8],
        partition_key: i32,
    ) -> Result<DoryMessageWithKey<'a>, &'static str> {
        let mut msg = DoryMessageWithKey {
            message_size: 0,
            api_key: 256,
            api_key_version: 0,
            flags: 0,
            topic_lenght: topic_value.len() as i16,
            timestamp: DoryMessageWithKey::get_time_miliseconds(),
            key_length: key_value.len() as i32,
            value_length: value.len() as i32,
            partition_key,
            topic_value,
            key_value,
            value,
        };
        msg.get_size();

        match DoryMessageAnyKey::check_size(msg.value, msg.topic_value) {
            Ok(_) => Ok(msg),
            Err(e) => Err(e),
        }
    }

    fn get_size(&mut self) {
        let message_size = size_of_val(&self.message_size)
            + size_of_val(&self.api_key)
            + size_of_val(&self.api_key_version)
            + size_of_val(&self.flags)
            + size_of_val(&self.partition_key)
            + size_of_val(&self.topic_lenght)
            + size_of_val(&self.timestamp)
            + size_of_val(&self.key_length)
            + size_of_val(&self.value_length)
            + self.topic_value.len()
            + self.key_value.len()
            + self.value.len();

        self.message_size = message_size as i32;
    }

    // Ideally would use bincode, but its a diffrent format than dory requires
    pub fn serialize(&self) -> Vec<u8> {
        let mut buff: Vec<u8> = Vec::new();

        buff.extend_from_slice(&self.message_size.to_be_bytes());
        buff.extend_from_slice(&self.api_key.to_be_bytes());
        buff.extend_from_slice(&self.api_key_version.to_be_bytes());
        buff.extend_from_slice(&self.flags.to_be_bytes());
        buff.extend_from_slice(&self.partition_key.to_be_bytes());
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
