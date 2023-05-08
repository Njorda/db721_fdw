use std::{fs::File, io::{Read, Seek, SeekFrom}, any::Any};
use std::collections::{HashMap, HashSet};
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
pub struct Metadata {
    #[serde(rename = "Table")]
    pub table: String,

    #[serde(rename = "Max Values Per Block")]
    pub max_values_per_block: i32,

    #[serde(rename = "Columns")]
    pub columns: HashMap<String, Columns>,
    // add the other fields if you need them
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
    return serde_json::from_slice(&buffer).unwrap();
}

// Should we return the data out in the format here using some? Lets first define exactly two columns :) 
#[derive(Debug)]
pub struct Frame{
    WeightG: Option<Vec<i32>>,
    AgeWeeks: Option<Vec<i32>>,
    Identifier: Option<Vec<i32>>
}
#[derive(Debug, PartialEq)]
pub enum FilterType{
    Greater,
    Less,
    Equal,
    Or,  
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Values{
    String(String),
    Int(i64),
    Float(f64)
}

#[derive(Debug)]
pub struct Filter {
    column: String,
    filter: FilterType,
    value: Values,
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

    pub fn filter(&self, filter:HashMap<String, Filter> ) -> Frame{

        // Here we filter out which blocks are of interest
        // Predicate push down
        let (skip_blocks, offsets) = self.block_filter(&filter); 

        // Here we start to read the data, 
        let mut f = File::open("/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721").unwrap();

        // Here we start with the values
        let mut out:Vec<i32> = Vec::new();

        println!("{:#?}", skip_blocks);

        // for the values we like to keep
        // Map[Block]Map[Row]bit
        let keep_value: HashMap<String, HashMap<String, i32>> = HashMap::new();
        let mut identifier_vec = Vec::new();

        // Make the vectors here and add to them in the end return the frame with the vectors takeing the ownershipt
        // seems easier!

        for (column, column_data) in self.columns.iter() {
            let Some(column_filter) = filter.get(column) else { continue; };
            for (block_index, _stats) in column_data.block_stats.iter(){
                println!("Start block: {}",block_index );
                if skip_blocks.contains(block_index){
                    println!("Skip block: {}", block_index);
                    continue;
                }

                // READ THE ACTUAL DATA

                // Here we read the data lets figure our later how we filter the other rows efficiently
                // Read the last 4 bytes to get the size of the metadata
                let block_index:i32 = block_index.parse().unwrap();
                let start:u64 = (column_data.start_offset*(1+block_index)).try_into().unwrap();
                f.seek(SeekFrom::Start(start)).unwrap(); // We want the last 4 bytes
                let block_bytes = if block_index == column_data.num_blocks{ first_after(&offsets,&column_data.start_offset)} else {
                    Some(self.max_values_per_block*4)
                };
                let block_bytes = block_bytes.unwrap(); // fix this later on. 
                let mut buffer = vec![0u8; block_bytes.try_into().unwrap()];
                f.read_exact(&mut buffer).unwrap();


                // PARSE THE BUFFER TO BYTES
                for i in (0..buffer.len()).step_by(4) {
                    // only handle int now
                    
                    match column_data.column_type.as_str() {
                        "int" =>{
                            let val = LittleEndian::read_i32(&buffer[i..]);
                            match  column_filter.filter {
                                FilterType::Equal => {
                                    if Values::Int(val as _) == column_filter.value{ 
                                        continue
                                    }
                                },
                                FilterType::Greater => 
                                    if Values::Int(val as _) <= column_filter.value{ 
                                        continue
                                    },
                                FilterType::Less =>                                     
                                    if Values::Int(val as _) >= column_filter.value{ 
                                        continue
                                },
                                FilterType::Or => (),
                            }
                            match column.as_str() {
                                "identifier"=>{
                                        identifier_vec.push(val);
                                },
                                _ => (),
                            }
                        }
                        "float" =>{
                            let val = LittleEndian::read_i32(&buffer[i..]);
                            match  column_filter.filter {
                                FilterType::Equal => {
                                    if Values::Float(val as _) == column_filter.value{ 
                                        continue
                                    }
                                },
                                FilterType::Greater => 
                                    if Values::Float(val as _) <= column_filter.value{ 
                                        continue
                                    },
                                FilterType::Less =>                                     
                                    if Values::Float(val as _) >= column_filter.value{ 
                                        continue
                                },
                                FilterType::Or => (),
                            }
                        }
                        "str" =>(),
                        _ => (),
                    }
                }
                // here we need to apply the filters and keep this for the other columns ...


            }
            break
        }
        // 2 read the data of interest
        println!("{:#?}", skip_blocks);
        println!("{:#?}", out.len());
        let frame = Frame{
            Identifier: Some(identifier_vec),
            AgeWeeks: None,
            WeightG: None,
        };
        return frame
    }


}



// take the filters as a list

// Make a read method? 

// Then start to separate out the code more and more I guess. 
// How do I want to structure it now ...



fn main() {
    println!("Hello World!");
    let meta = read_metadata("/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721".to_string());
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
    println!("{:?}", Some(meta.filter(mappy).Identifier));


    // next step is to further understand the format of the blocks.
    // and then then we can start to build logic for strings and so on
    // we can use the length, the max mean and so on. 

}