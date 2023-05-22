use std::{fs::File, io::{Read, Seek, SeekFrom}, any::Any};
use std::collections::{HashMap, HashSet};
use pgrx::pg_sys::{self, Datum, Oid};
use pgrx::*;
use serde::{Deserialize, Serialize};
use byteorder::{ByteOrder, LittleEndian};
use serde_json::{Value, Map, Number}; //https://stackoverflow.com/questions/39146584/how-do-i-create-a-rust-hashmap-where-the-value-can-be-one-of-multiple-types


// There will be one server, one wrapper and so on
// But we will create to tables.
// We will use the options, when the table is create as a way to say which file do we want to query ... this should be is I guess
// Check the scripts they have for installation .. 

/// a struct into which to decode the thing
#[derive(Serialize, Deserialize, Debug)]
pub struct Columns{
    #[serde(rename = "type")]
    column_type: String,

    start_offset: i32,

    num_blocks: i32,

    block_stats: HashMap<String, HashMap<String, Value>,>,
}

/// a struct into which to decode the thing
#[derive(Serialize, Deserialize, Debug)]
pub struct db721_Metadata {
    #[serde(rename = "Table")]
    pub table: String,

    #[serde(rename = "Max Values Per Block")]
    pub max_values_per_block: i32,

    #[serde(rename = "Columns")]
    pub columns: HashMap<String, Columns>,

    // add the other fields if you need them
}

pub struct Metadata{
    pub table: String,

    pub max_values_per_block: i32,

    pub columns: HashMap<String, Columns>,

    pub start_metadata: i32
}

//"/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721"
pub fn read_metadata(filename: String)-> Metadata {
    let mut f = File::open(filename).unwrap();
    // Read the last 4 bytes to get the size of the metadata
    f.seek(SeekFrom::End(-4)).unwrap(); // We want the last 4 bytes
    let mut buffer = [0u8; std::mem::size_of::<u32>()]; // could just hard code to 4 as well, but should not matter here at all
    f.read_exact(&mut buffer).unwrap();
    let out = i32::from_le_bytes(buffer);
    // https://stackoverflow.com/questions/68220659/get-json-value-from-byte-using-rust
    // Read the meta data json object to a rust struct
    f.seek(SeekFrom::End(-i64::from(out)-4)).unwrap();
    let mut buffer = vec![0u8; out.try_into().unwrap()];
    f.read_exact(&mut buffer).unwrap();
    let meta: db721_Metadata =  serde_json::from_slice(&buffer).unwrap();
    let file_length: i32 = f.metadata().unwrap().len().try_into().unwrap();
    let start_metadata =  file_length - i32::from(out)-4;
    return Metadata { table: meta.table, max_values_per_block: meta.max_values_per_block, columns: meta.columns, start_metadata: start_metadata}
}

// Should we return the data out in the format here using some? Lets first define exactly two columns :) 
#[derive(Debug)]
// This one needs to become generic it can not be a static type as we have here since it will vary between. 
pub struct Frame{
    pub WeightG: Option<Vec<i32>>,
    pub AgeWeeks: Option<Vec<i32>>,
    pub Identifier: Option<Vec<i32>>
}
#[derive(Debug, PartialEq, Clone)]
pub enum FilterType{
    Greater,
    Less,
    Equal,
    Or,  
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Values{
    String(String),
    Int(i64),
    Float(f64)
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub filter: FilterType,
    pub value: Values,
}


fn inside(stats: &HashMap<String,Value>, filter: &Filter, data_type: &String) -> bool {
    match data_type.as_str() {
        "int" => {
            match filter.filter {
                FilterType::Equal => {
                    match stats.get("min") {
                        None => (),
                        Some(block_min) => {
                                if let Some(block_min) =  block_min.as_i64(){
                                    if Values::Int(block_min) > filter.value{ 
                                        return false
                                    }
                                else{}
                            } 
                        },
                    }
                    match stats.get("max") {
                        None => (),
                        Some(block_max) => {
                                if let Some(block_max) =  block_max.as_i64(){
                                    if Values::Int(block_max) < filter.value{
                                        return false
                                    }
                                else{}
                            } 
                        },
                    }
                },
                FilterType::Greater => {
                    match stats.get("max") {
                        None => (),
                        Some(block_max) => {
                                if let Some(block_max) =  block_max.as_i64(){
                                    if Values::Int(block_max) <= filter.value{
                                        return false
                                    }
                                else{}
                            } 
                        },
                    }
                }
                FilterType::Less  => {
                    match stats.get("min") {
                        None => (),
                        Some(block_min) => {
                                if let Some(block_min) =  block_min.as_i64(){
                                    if Values::Int(block_min) >= filter.value{
                                        return false
                                    }
                                else{}
                            } 
                        },
                    }
                },
                FilterType::Or  => (),

            }
        },
        "float" => (),
        "string" => (),
        "str" => (),
        _ => panic!("unknown type: {}", data_type)

    }

    // -------------------------------------------------------------
    //             |                    |
    //         block min              block max
    //     | filter
    //                      | filter
    //                                         | filter

// Order the code using the following
// Type data type, get from the meta data
// Then on operator, get from the filter
// Then we have the logic, get from the filter as well, strings will be very different here, might be faaaast if we check length of it as well! 


    return true
}


fn first_after(offsets: &Vec<i32>, value: &i32) -> Option<i32>{
    let mut curr = false;
    for val in offsets{
        if curr {
            return Some(val.clone())
        }
        if val == value{
            curr = true
        }
    }
    return None
}

impl Metadata{
    pub fn block(&self) -> i32{
        return self.max_values_per_block.clone()
    }


    pub fn block_filter(&self, filter:&HashMap<String, Filter> ) -> (HashSet<String>,  Vec<i32>) {

        let mut blocks = Vec::<String>::new();
        let mut offsets = Vec::<i32>::new();

        offsets.push(self.start_metadata);

        for (column, columnData) in self.columns.iter() {
            if filter.contains_key(column){
                let Some(column_filter) = filter.get(column) else { todo!() };
                for (block_index, stats) in columnData.block_stats.iter(){
                    if inside(&stats, column_filter, &columnData.column_type) {
                        blocks.push(block_index.to_string())
                    }
                }
            }
            offsets.push(columnData.start_offset)
        }

        let skip_blocks:HashSet<String> = HashSet::from_iter(blocks.iter().cloned());
        offsets.sort();
        return (skip_blocks, offsets)
    }

    pub fn filter(&self, filter:HashMap<String, Filter> ) -> HashMap<String, Vec<Datum>>{

        // Here we filter out which blocks are of interest
        // Predicate push down
        let (skip_blocks, offsets) = self.block_filter(&filter); 

        // Here we start to read the data, 
        let mut f = File::open("/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721").unwrap();

        println!("skip blocks {:#?}", skip_blocks);

        // for the values we like to keep
        // Map[Block]Map[Row]bit
        let keep_value: HashMap<String, HashMap<String, i32>> = HashMap::new();

        // Make the vectors here and add to them in the end return the frame with the vectors takeing the ownershipt
        // seems easier!

        // How to do this
        // Load all data
        // For each create the array of bitmaps
        // Combine the bitmaps
        // Filter all the data
        // Pass the data out. 

        let mut out:HashMap<String, Vec<Datum>> = HashMap::new();
        let mut bit_maps:HashMap<String, Vec<bool>> = HashMap::new();
        let mut bit_map:Vec<bool> = Vec::new();

        for (column, column_data) in self.columns.iter() {
            let mut vector:Vec<Datum> = Vec::new();
            bit_map= Vec::new();

            for (block_index, _stats) in column_data.block_stats.iter(){
                log!("Start block: {}",block_index );
                if skip_blocks.contains(block_index){
                    log!("Skip block: {}", block_index);
                    continue;
                }

                // READ THE ACTUAL DATA

                // WE DONT HANDLE STR CORRECT!!!!
                // THIS IS AN ISSUE!!!!!
                // WE DONT HANDLE THE LAST BYTES CORRECT!!!!

                // Here we read the data lets figure our later how we filter the other rows efficiently
                // Read the last 4 bytes to get the size of the metadata
                let block_index:i32 = block_index.parse().unwrap();
                let start:u64 = (column_data.start_offset +(block_index*self.max_values_per_block*4)).try_into().unwrap();
                f.seek(SeekFrom::Start(start)).unwrap(); // We want the last 4 bytes
                log!("The block: {}, the number of blocks: {}", block_index, column_data.num_blocks);


                // let block_bytes = if block_index == (column_data.num_blocks-1){ 
                //         log!("Inside the check, offset: {:?}, first after: {}", offsets, &column_data.start_offset);

                //         // CHECK HERE IF NONE use end of the blocks(remove the meta data and so on)
                //         // Continue here
                //         let block_bytes = first_after(&offsets,&column_data.start_offset).unwrap();
                //         block_bytes - (column_data.start_offset +(block_index*self.max_values_per_block*4))
                //     } else {
                //         self.max_values_per_block*4
                // };

                // log!("Start to read from: {}, reading bytes: {}", start, block_bytes);
                // let mut buffer = vec![0u8; block_bytes.try_into().unwrap()];
                // log!("block_bytes: {}, total nbr of blocks: {}, max values: {}, column: {}", block_bytes, column_data.num_blocks, self.max_values_per_block*4, column);
                // f.read_exact(&mut buffer).unwrap();
                // log!("IS THIS THE ISSUE 3");

                // CONTINUE HERE
                // WE could reuse a buffer to reduce memory allocations
                let buffer = reader(&mut f, block_index, column_data, &offsets, self.max_values_per_block);

                // PARSE THE BUFFER TO BYTES
                for i in (0..buffer.len()).step_by(4) {
                    // only handle int now
                    match column_data.column_type.as_str() {
                        "int" =>{
                            let val = LittleEndian::read_i32(&buffer[i..]);
                            if let Some(column_filter) = filter.get(column){
                                // this is for the bitmap stuff
                                match  column_filter.filter {
                                    FilterType::Equal => {
                                        if Values::Int(val as _) == column_filter.value{ 
                                            bit_map.push(false);
                                        }
                                    },
                                    FilterType::Greater => 
                                        if Values::Int(val as _) <= column_filter.value{ 
                                            bit_map.push(false);
                                        },
                                    FilterType::Less =>                                     
                                        if Values::Int(val as _) >= column_filter.value{ 
                                            bit_map.push(false);
                                    },
                                    FilterType::Or => (),
                                }
                            } else {
                                bit_map.push(true);
                            };
                            if let Some(d_val) =  val.into_datum(){
                                vector.push(d_val);
                            }
                        }
                        "float" =>{
                            let val = LittleEndian::read_i32(&buffer[i..]);
                            if let Some(column_filter) = filter.get(column){
                                match  column_filter.filter {
                                    FilterType::Equal => {
                                        if Values::Float(val as _) == column_filter.value{ 
                                            bit_map.push(false);
                                        }
                                    },
                                    FilterType::Greater => 
                                        if Values::Float(val as _) <= column_filter.value{ 
                                            bit_map.push(false);
                                        },
                                    FilterType::Less =>                                     
                                        if Values::Float(val as _) >= column_filter.value{ 
                                            bit_map.push(false);
                                    },
                                    FilterType::Or => (),
                                }
                                if let Some(d_val) =  val.into_datum(){
                                    vector.push(d_val);
                                }
                            } else {
                                bit_map.push(true);
                            };
                        }
                        "str" =>(),
                        _ => (),
                    }
                }
                // here we need to apply the filters and keep this for the other columns ...
            }
            out.insert(column.to_string(), vector);
            bit_maps.insert(column.to_string(), bit_map.clone());
        }

        // Continue here with joining the bitmaps to one! 
        for (k,v) in bit_maps.iter(){
            bit_map = v
                .iter()
                .zip(v.iter())
                .map(|(v, c)| if c | v { false } else { true })
                .collect();
        }
        let tmp: HashMap<String, Vec<Datum>> = HashMap::new();
        for (k,v) in out.iter(){
            let vals:Vec<Datum> = v.iter()
            .zip(bit_map.iter().copied())
            .filter(|(v,c)| c.clone())
            .map(|(v, c)| *v)
            .collect();
        }

        // 2 read the data of interest
        println!("{:#?}", skip_blocks);
        println!("{:#?}", out.len());
        return out
    }


}

fn reader(f: &mut File, block_index: i32, column_data: &Columns, offsets: &Vec<i32>, max_values_per_block: i32) -> Vec<u8> {
    let multiple =  match column_data.column_type.as_str(){
        "int" => 4, 
        "float" => 4,
        "str" => 32,
        _ => 4,
    };

    let block_bytes = if block_index == (column_data.num_blocks-1){ 
            log!("Inside the check, offset: {:?}, first after: {}", offsets, &column_data.start_offset);

            // CHECK HERE IF NONE use end of the blocks(remove the meta data and so on)
            // Continue here
            let block_bytes = first_after(&offsets,&column_data.start_offset).unwrap();
            block_bytes - (column_data.start_offset +(block_index*max_values_per_block*multiple))
        } else {
            max_values_per_block*multiple
    };

    let mut buffer = vec![0u8; block_bytes.try_into().unwrap()];
    log!("block_bytes: {}, total nbr of blocks: {}, max values: {}", block_bytes, column_data.num_blocks, max_values_per_block*multiple);
    f.read_exact(&mut buffer).unwrap();
    log!("IS THIS THE ISSUE 3");
    return buffer;
}

// take the filters as a list

// Make a read method? 

// Then start to separate out the code more and more I guess. 
// How do I want to structure it now ...



fn main() {
    println!("Hello World!");
    let meta= read_metadata("/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721".to_string());
    println!("{}", meta.table);
    for (column, value) in meta.columns.iter() {
        println!("the column is: {}, the start offset is: {}, nbr blocks {}", column, value.start_offset, value.num_blocks);
        for (block_index, stats) in value.block_stats.iter(){
            println!("The block is: {} data type: {}", block_index, value.column_type);
            for (key, values) in stats.into_iter(){
                let val = value.column_type.as_str();
                let column_type: Vec<&str> = val.split(" / ").collect();
                let vals = column_type[0];
                match vals{
                "str"=>println!("str:{} {:#?}", key, values.as_str()),
                "int"=>println!("int:{} {:#?}", key, values.as_i64()),
                "float"=>println!("float:{} {:#?}", key, values.as_f64()),
                &_ => println!("Not handled")
                }
            }
        }
        break;
    }
    println!("number of blocks: {}", meta.max_values_per_block);
    println!("Here we go {}", meta.block());
    let filter = Filter{
        column: "identifier".to_string(),
        filter: FilterType::Equal,
        value: Values::Int(10),
    };
    let mut mappy: HashMap<String, Filter> = HashMap::new();
    mappy.insert("identifier".to_string(), filter);
    println!("{:?}", Some(meta.filter(mappy)));


    // next step is to further understand the format of the blocks.
    // and then then we can start to build logic for strings and so on
    // we can use the length, the max mean and so on. 

}