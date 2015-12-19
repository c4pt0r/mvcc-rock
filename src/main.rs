#![feature(convert)]
extern crate byteorder;
extern crate rocksdb;

use byteorder::{BigEndian, WriteBytesExt};

struct Error(i32, String);
type Result<T> = std::result::Result<T, Error>;

fn encode(key :&[u8], ver :u64) -> Result<Vec<u8>> {
    let mut data = key;
    let mut new_key = Vec::with_capacity(key.len() + 20);

    if data.len() > 0 && data[0] == b'\xff' {
        // we must escape 0xFF here to guarantee encoded value < InfiniteValue \xFF\xFF.
        new_key.push(b'\xff');
        new_key.push(b'\x00');
        data = &data[1..];
    }
    for c in data {
        match *c {
            b'\x00' =>  {
                new_key.push(b'\x00');
                new_key.push(b'\xff');
            }
            _ => {
                new_key.push(*c);
            }
        }
    }
    new_key.push(b'\x00');
    new_key.push(b'\x01');

    let mut wtr = Vec::with_capacity(8);
    wtr.write_u64::<BigEndian>(ver).unwrap();
    new_key.extend(&*wtr);

    Ok(new_key)
}

fn decode(encoded :&[u8]) -> Result<(Vec<u8>, u64)> {
    let mut data = encoded;
    let mut key = Vec::new();

    if data[0] == b'\xff' {
        if data[1] != b'\x00' {
            return Err(Error(-1, "invalid format".to_string()))
        }
        key.push(data[0]);
        data = &data[2..]
    }

    let mut ver_buf :&[u8];
    'outer: loop {
        let mut idx = 0;
        for (i, c) in data.iter().enumerate() {
            if *c == b'\x00' {
                idx = i;
                if data[i + 1] == b'\x01' {
                    key.extend(&data[..idx]);
                    ver_buf = &data[idx+2..];
                    break 'outer;
                }
                break
            }
        }
        key.extend(&data[..idx]);
        key.push(b'\x00');
        data = &data[idx+2..];
    }

    // TODO: deal with ver_buf
    Ok((key, 0))
}

trait MvccDB {
    fn put(&self, key :&[u8], val :&[u8], ver :u64) -> Result<()>;
    fn get(&self, key :&[u8], ver :u64) -> Result<&[u8]>;
}

struct MvccRocks;
impl MvccDB for MvccRocks {
    fn put(&self, key :&[u8], val :&[u8], ver :u64) -> Result<()> {
        println!("put {:?}, {:?}, {}", key, val, ver);
        Ok(())
    }

    fn get(&self, key :&[u8], ver :u64) -> Result<&[u8]> {
        println!("get {:?}, {}", key, ver);
        Ok(b"hello")
    }
}

fn main() {
    let b = Box::new(MvccRocks);
    match b.put(b"hello", b"world", 1) {
        Ok(()) => println!("ok"),
        Err(Error(code, msg)) => println!("err code: {:?}, msg: {:?}", code, msg),
    }

    let ori = b"\xff\xffh\x00e\x00llo";
    match encode(ori, 23) {
        Ok(key) => {
            println!("encoded: {:?}", key);
            match decode(key.as_slice()) {
                Ok((r, ver)) => {
                    println!("ori:{:?} decoded: {:?}", ori, r);
                }
                _ => (),
            }
        }
        _ => (),
    }
}
