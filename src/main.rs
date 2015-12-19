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

    // put version
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
    for (i, c) in data.iter().enumerate() {
        // find \x00 => {\xff, \x01 -> term}
        match *c {
            b'\x00' => {
                if i + 1 >= data.len() {
                    return Err(Error(-2, "invalid format".to_string()))
                }
                match data[i+1] {
                    b'\xff' => {
                        key.push(b'\x00');
                        continue
                    }
                    b'\x01' => {
                        break
                    }
                    _ => {
                        return Err(Error(-3, "invalid format".to_string()))
                    }
                }
            }
            _ => {
                key.push(*c);
            }
        }
    }
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

    match encode(b"\xffhe\x00llo", 23) {
        Ok(key) => {
            println!("encoded: {:?}", key);
            match decode(key.as_slice()) {
                Ok((ori, ver)) => {
                    println!("decoded: {:?}", ori);
                }
                _ => (),
            }
        }
        _ => (),
    }
}
