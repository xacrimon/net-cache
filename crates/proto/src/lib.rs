use anyhow::{Result, bail};
use redis_protocol::resp2::{
    decode::decode as decode_resp2,
    types::{OwnedFrame, Resp2Frame},
};

pub enum Request {}

pub struct RequestDecoder {
    buf: Vec<u8>,
    data_len: usize,
}

impl RequestDecoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            data_len: 0,
        }
    }

    pub fn read_at(&mut self, read_buf_size: usize) -> &mut [u8] {
        let free = self.buf.len() - self.data_len;
        if free < read_buf_size {
            let additional = read_buf_size - free;
            let new_len = self.buf.len() + additional;
            self.buf.resize(new_len, 0);
        }

        debug_assert!(self.buf.len() == self.data_len + read_buf_size);
        debug_assert!(&self.buf[..read_buf_size].iter().all(|&b| b == 0));

        &mut self.buf[..read_buf_size]
    }

    pub fn advance(&mut self, n: usize) {
        self.data_len += n;
        debug_assert!(self.data_len <= self.buf.len());
    }

    fn next_frame(&mut self) -> Result<Option<OwnedFrame>> {
        let buf = &self.buf[..self.data_len];

        if let Some((frame, consumed)) = decode_resp2(buf)? {
            self.buf.drain(0..consumed);
            self.data_len -= consumed;
            return Ok(Some(frame));
        }

        Ok(None)
    }

    pub fn try_decode(&mut self) -> Result<Option<Request>> {
        let frame = match self.next_frame()? {
            Some(frame) => frame,
            None => return Ok(None),
        };

        todo!()
    }
}

pub enum Response {}
