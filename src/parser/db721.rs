use std::{fs::File, io::{Read, Seek, SeekFrom}, str};
use std::collections::{HashMap, HashSet};
use pgrx::pg_sys::{self, Datum, Oid};
use pgrx::*;
use std::fmt;
use serde::{Deserialize, Serialize};
use byteorder::{ByteOrder, LittleEndian};
use serde_json::Value; //https://stackoverflow.com/questions/39146584/how-do-i-create-a-rust-hashmap-where-the-value-can-be-one-of-multiple-types


// There will be one server, one wrapper and so on
// But we will create to tables.
// We will use the options, when the table is create as a way to say which file do we want to query ... this should be is I guess
// Check the scripts they have for installation .. 

/// a struct into which to decode the thing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Columns{
    #[serde(rename = "type")]
    column_type: String,

    start_offset: i32,

    num_blocks: i32,

    block_stats: HashMap<String, HashMap<String, Value>,>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnMetadada {
    /// column name
    pub name: String,
    /// 1-based column number
    pub num: usize,
    /// column type OID, can be used to match pg_sys::BuiltinOid
    pub type_oid: pg_sys::Oid,
}

/// a struct into which to decode the thing
#[derive(Serialize, Deserialize, Debug)]
pub struct Db721Metadata {
    #[serde(rename = "Table")]
    pub table: String,

    #[serde(rename = "Max Values Per Block")]
    pub max_values_per_block: i32,

    #[serde(rename = "Columns")]
    pub columns: HashMap<String, Columns>,

    // add the other fields if you need them
}

#[derive(Clone, Debug)]
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
    let meta: Db721Metadata =  serde_json::from_slice(&buffer).unwrap();
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

        for (column, column_data) in self.columns.iter() {
            if filter.contains_key(column){
                let Some(column_filter) = filter.get(column) else { todo!() };
                for (block_index, stats) in column_data.block_stats.iter(){
                    if inside(&stats, column_filter, &column_data.column_type) {
                        blocks.push(block_index.to_string())
                    }
                }
            }
            offsets.push(column_data.start_offset)
        }

        let skip_blocks:HashSet<String> = HashSet::from_iter(blocks.iter().cloned());
        offsets.sort();
        return (skip_blocks, offsets)
    }

    // Continue here to package the columns in to the correct format so we can


    pub fn tuples(&self, data: &HashMap<String, Vec<Cell>>, cols: Vec<ColumnMetadada>) -> Vec<Vec<Cell>>{
        // Make tuples of it so we get then in 
        // we need to make it correct. 
        // let mut tuples= Vec::new();

        // 1) Create the vector of the set length
        // 2) Create a tuple in each position of length==nbr of columns
        // 3) Populate the values correctly

        log!("The number of columns are: {}, the number of columns in the schema are: {}", data.len(), cols.len());
        let mut length = 0;
        for (_key, val) in data.iter(){
            if length == 0 {
                length = val.len()
            }
            if length != val.len(){
                log!("DIFFERENT LENGTH")
            }
            length = val.len()
        }
        let mut out = vec![vec![Cell::Bool(true); cols.len()]; length];
        for (tuple_idx, col) in cols.iter().enumerate(){
            if let Some(val) = data.get(col.name.as_str()){
                for (value_idx, v) in val.iter().enumerate(){
                    out[value_idx][tuple_idx] = v.clone()
                }
            }
        }
    return out
    }

    pub fn filter(&self, filter:HashMap<String, Filter> ) -> HashMap<String, Vec<Cell>>{

        // Here we filter out which blocks are of interest
        // Predicate push down
        let (skip_blocks, offsets) = self.block_filter(&filter); 

        // Here we start to read the data, 
        let mut f = File::open("/Users/niklashansson/OpenSource/postgres/cmudb/extensions/db721_fdw/data-chickens.db721").unwrap();

        println!("skip blocks {:#?}", skip_blocks);
        // Make the vectors here and add to them in the end return the frame with the vectors takeing the ownershipt
        // seems easier!

        // How to do this
        // Load all data
        // For each create the array of bitmaps
        // Combine the bitmaps
        // Filter all the data
        // Pass the data out. 

        let mut out:HashMap<String, Vec<Cell>> = HashMap::new();
        let mut bit_maps:HashMap<String, Vec<bool>> = HashMap::new();
        let mut bit_map:Vec<bool> = Vec::new();

        for (column, column_data) in self.columns.iter() {
            let mut vector:Vec<Cell> = Vec::new();
            bit_map= Vec::new();

            for (block_index, _stats) in column_data.block_stats.iter(){
                log!("Start block: {}",block_index );
                if skip_blocks.contains(block_index){
                    log!("Skip block: {}", block_index);
                    continue;
                }
                let block_index:i32 = block_index.parse().unwrap();
                let start:u64 = (column_data.start_offset +(block_index*self.max_values_per_block*4)).try_into().unwrap();
                f.seek(SeekFrom::Start(start)).unwrap(); // We want the last 4 bytes
                
                let (buffer, step_size) = reader(&mut f, block_index, column_data, &offsets, self.max_values_per_block);
                log!("The block: {}, the number of blocks: {}, and step size: {}", block_index, column_data.num_blocks, step_size);

                // PARSE THE BUFFER TO BYTES
                for i in (0..buffer.len()).step_by(step_size.try_into().unwrap()) {
                    // only handle int now
                    // we could reduce this branching here potentially .. 
                    // Also the steps for str are 32 not 4 ... 
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
                            vector.push(Cell::I32(val));
                        }
                        "float" =>{
                            let val = LittleEndian::read_f32(&buffer[i..]);
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
                            } else {
                                bit_map.push(true);
                            };
                            vector.push(Cell::F32(val));
                        }
                        "str" =>{
                            let val = str::from_utf8(&buffer[i..i+31]).unwrap();                            
                            bit_map.push(true);
                            vector.push(Cell::String(val.to_string()));
                        },
                        _ => (),
                    }
                }
                // here we need to apply the filters and keep this for the other columns ...
            }
            out.insert(column.to_string(), vector);
            bit_maps.insert(column.to_string(), bit_map.clone());
        }

        // Continue here with joining the bitmaps to one! 
        for (_k,v) in bit_maps.iter(){
            bit_map = v
                .iter()
                .zip(v.iter())
                .map(|(v, c)| if c | v { false } else { true })
                .collect();
        }
        // let tmp: HashMap<String, Vec<Cell>> = HashMap::new();
        // for (k,v) in out.iter(){
        //     let vals:Vec<Cell> = v.iter()
        //     .zip(bit_map.iter().copied())
        //     .filter(|(v,c)| c.clone())
        //     .map(|(v, c)| *v)
        //     .collect();
        // }

        // 2 read the data of interest
        println!("{:#?}", skip_blocks);
        println!("{:#?}", out.len());
        return out
    }


}

fn reader(f: &mut File, block_index: i32, column_data: &Columns, offsets: &Vec<i32>, max_values_per_block: i32) -> (Vec<u8>, i32) {
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
    return (buffer, multiple);
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




/// A data cell in a data row
#[derive(Debug)]
pub enum Cell {
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(AnyNumeric),
    String(String),
    Date(Date),
    Timestamp(Timestamp),
    Json(JsonB),
}

impl Clone for Cell {
    fn clone(&self) -> Self {
        match self {
            Cell::Bool(v) => Cell::Bool(*v),
            Cell::I8(v) => Cell::I8(*v),
            Cell::I16(v) => Cell::I16(*v),
            Cell::F32(v) => Cell::F32(*v),
            Cell::I32(v) => Cell::I32(*v),
            Cell::F64(v) => Cell::F64(*v),
            Cell::I64(v) => Cell::I64(*v),
            Cell::Numeric(v) => Cell::Numeric(v.clone()),
            Cell::String(v) => Cell::String(v.clone()),
            Cell::Date(v) => Cell::Date(v.clone()),
            Cell::Timestamp(v) => Cell::Timestamp(v.clone()),
            Cell::Json(v) => Cell::Json(JsonB(v.0.clone())),
        }
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cell::Bool(v) => write!(f, "{}", v),
            Cell::I8(v) => write!(f, "{}", v),
            Cell::I16(v) => write!(f, "{}", v),
            Cell::F32(v) => write!(f, "{}", v),
            Cell::I32(v) => write!(f, "{}", v),
            Cell::F64(v) => write!(f, "{}", v),
            Cell::I64(v) => write!(f, "{}", v),
            Cell::Numeric(v) => write!(f, "{:?}", v),
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::Date(v) => write!(f, "{:?}", v),
            Cell::Timestamp(v) => write!(f, "{:?}", v),
            Cell::Json(v) => write!(f, "{:?}", v),
        }
    }
}

impl IntoDatum for Cell {
    fn into_datum(self) -> Option<Datum> {
        match self {
            Cell::Bool(v) => v.into_datum(),
            Cell::I8(v) => v.into_datum(),
            Cell::I16(v) => v.into_datum(),
            Cell::F32(v) => v.into_datum(),
            Cell::I32(v) => v.into_datum(),
            Cell::F64(v) => v.into_datum(),
            Cell::I64(v) => v.into_datum(),
            Cell::Numeric(v) => v.into_datum(),
            Cell::String(v) => v.into_datum(),
            Cell::Date(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Json(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
       return Oid::type_oid();
    }
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            return None;
        }
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Some(Cell::Bool(bool::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
                Some(Cell::I8(i8::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                Some(Cell::I16(i16::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                Some(Cell::F32(f32::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                Some(Cell::I32(i32::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                Some(Cell::F64(f64::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                Some(Cell::I64(i64::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                Some(Cell::Numeric(AnyNumeric::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
                Some(Cell::String(String::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Some(Cell::Date(Date::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => Some(Cell::Timestamp(
                Timestamp::from_datum(datum, false).unwrap(),
            )),
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                Some(Cell::Json(JsonB::from_datum(datum, false).unwrap()))
            }
            _ => None,
        }
    }
}
