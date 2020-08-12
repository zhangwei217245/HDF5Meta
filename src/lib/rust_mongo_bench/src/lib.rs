#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate serde_json;


#[macro_use]
extern crate lazy_static;
extern crate libc;


use std::ffi::CStr;
use libc::c_char;
use bson::Bson;
use bson::Document;
use mongodb::ordered::OrderedDocument;
use mongodb::{Client, ThreadedClient, CommandResult};
use mongodb::db::{ThreadedDatabase};
use mongodb::cursor::Cursor;
use mongodb::coll::Collection;
use serde_json::Value;

lazy_static! {
    static ref MONGO_COLL: Collection = {
        // Connect mongo with the following command if on NERSC
        // mongo mongodb03.nersc.gov/HDF5MetadataTest -u HDF5MetadataTest_admin -p ekekek19294jdwss2k
        
        let mut client = Client::with_uri("mongodb://HDF5MetadataTest_admin:ekekek19294jdwss2k@mongodb03.nersc.gov/HDF5MetadataTest")
        .expect("Failed on connection");
        client.add_completion_hook(log_query_duration).unwrap();
        let db = client.db("HDF5MetadataTest");
        db.auth("HDF5MetadataTest_admin","ekekek19294jdwss2k").unwrap();
        db.collection("abcde")
    };
}

fn log_query_duration(_client: Client, command_result: &CommandResult) {
    match command_result {
        &CommandResult::Success { duration, reply:_, ref command_name, .. } => {
            println!("Command {} took {} nanoseconds.", command_name, duration);
        },
        _ => println!("Failed to execute command."),
    }
}

fn c_str_to_r_str(c_string_ptr: *const c_char) -> String {
    let c_str = unsafe {
        assert!(!c_string_ptr.is_null());
        CStr::from_ptr(c_string_ptr)
    };
    c_str.to_str().unwrap().to_owned()
}

fn c_str_to_bson(c_string_ptr: *const c_char) -> Document{
    let r_str = c_str_to_r_str(c_string_ptr);
    // let string_count = r_str.len() as i32;
    let json : Value = serde_json::from_str(&r_str).unwrap();
    let bson : Bson = json.into();
    bson.as_document().unwrap().clone()
}

#[no_mangle]
pub extern fn init_db() -> i64 {
    let query_doc = doc!{};
    let db_count = MONGO_COLL.count(Some(query_doc), None).unwrap();
    println!("db count = {}", db_count);
    db_count
}

#[no_mangle]
pub extern "C" fn clear_all_docs() -> i64 {
    MONGO_COLL.delete_many(doc!{}, None).unwrap().deleted_count as i64
}

#[no_mangle]
pub extern "C" fn clear_all_indexes() {
    MONGO_COLL.drop_indexes().unwrap();
}

#[no_mangle]
pub extern "C" fn drop_current_coll() {
    MONGO_COLL.drop().unwrap();
}

#[no_mangle]
pub extern "C" fn create_index(index_key: *const c_char) {
    let doc = c_str_to_bson(index_key);
    MONGO_COLL.create_index(doc, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_doc_id_index() {
    MONGO_COLL.create_index(doc!{
        "h5doc_id" => 1
    }, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_dataset_name_index() {
    MONGO_COLL.create_index(doc!{
        "sub_objects.sub_objects.sub_objects.dataset_name" => 1
    }, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_root_obj_path_index() {
    MONGO_COLL.create_index(doc!{
        "object_path" => 1
    }, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_lv2_obj_path_index(){
    MONGO_COLL.create_index(doc!{
        "sub_objects.object_path" => 1
    }, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_lv3_obj_path_index(){
    MONGO_COLL.create_index(doc!{
        "sub_objects.sub_objects.object_path" => 1
    }, None).unwrap();
}

#[no_mangle]
pub extern "C" fn create_any_index(index_str: *const c_char){
    let attr_name = c_str_to_r_str(index_str);
    let mut doc = OrderedDocument::new();
    doc.insert(attr_name, 1);
    MONGO_COLL.create_index(doc, None).unwrap();
}

#[no_mangle]
pub extern "C" fn query_count(query_condition: *const c_char) -> i64 {
    let doc = c_str_to_bson(query_condition);
    MONGO_COLL.count(Some(doc), None).unwrap()
}

fn query_result_set(query_condition: *const c_char) -> Cursor {
    let doc = c_str_to_bson(query_condition);
    MONGO_COLL.find(Some(doc), None).unwrap()
}

#[no_mangle]
pub extern "C" fn query_result_count(query_condition: *const c_char) -> i64 {
    let result_set = query_result_set(query_condition);
    result_set.count() as i64
}

#[no_mangle]
pub extern "C" fn query_result_and_print(query_condition: *const c_char) {
    let mut result_set = query_result_set(query_condition);
    while result_set.has_next().unwrap() {
        let item = result_set.next();

        match item {
            Some(Ok(doc)) => match doc.get("object_path") {
                Some(&Bson::String(ref object_path)) => println!("object_path = {}", object_path),
                _ => panic!("Expected title to be a string!"),
            },
            Some(Err(_)) => panic!("Failed to get next from server!"),
            None => panic!("Server returned no results!"),
        } 
    }
}

#[no_mangle]
pub extern "C" fn get_all_doc_count() -> i64 {
    MONGO_COLL.count(Some(doc!{}), None).unwrap()
}

#[no_mangle]
pub extern "C" fn split_sub_objects_to_db (json_str: *const c_char) -> i64 {
    let doc = c_str_to_bson(json_str);
    let key = String::from("sub_objects");
    let bson_vec: Vec<Bson> = doc.get_array(&key).unwrap().to_owned();
    let array : Vec<Document> = bson_vec.into_iter().map(|b| b.as_document().unwrap().to_owned()).collect();
    MONGO_COLL.insert_many(array.clone(), None)
    .ok().expect("Failed to insert many documents.");
    array.clone().len() as i64
}

#[no_mangle]
pub extern "C" fn importing_json_doc_to_db (json_str: *const c_char) -> i64 {
    let doc = c_str_to_bson(json_str);    
    MONGO_COLL.insert_one(doc.clone(), None)
    .ok().expect("Failed to insert document.");
    1
}

#[no_mangle]
pub extern "C" fn importing_fake_json_docs_to_db (json_str: *const c_char, count: i32) -> i64 {
    let mut doc = c_str_to_bson(json_str);
    // inserting 1M documents. if mongo is not large enough, we try to shrink this by 1/10.
    for x in 0..count {
        doc.insert("doc_serialID".to_owned(), Bson::I32(x));
        MONGO_COLL.insert_one(doc.clone(), None)
        .ok().expect("Failed to insert document.");
    }
    let all_count = get_all_doc_count();
    println!("================ Total # of document is : {} ================", all_count);
    all_count
}

#[no_mangle]
pub extern fn random_test() {
    for n in (1..4).rev() {
        println!("Hello,my ssss world! {}", n);
    }

    MONGO_COLL.insert_one(doc!{ "title" => "Back to the Future" }, None).unwrap();
    let doc = doc! {
        "title": "Jaws",
        "array": [ 1, 2, 3 ],
    };
    // Insert document into 'test.movies' collection

    MONGO_COLL.insert_one(doc.clone(), None)
        .ok().expect("Failed to insert document.");

    // Find the document and receive a cursor
    let mut cursor = MONGO_COLL.find(Some(doc.clone()), None)
        .ok().expect("Failed to execute find.");

    let item = cursor.next();

    // cursor.next() returns an Option<Result<Document>>
    match item {
        Some(Ok(doc)) => match doc.get("title") {
            Some(&Bson::String(ref title)) => println!("{}", title),
            _ => panic!("Expected title to be a string!"),
        },
        Some(Err(_)) => panic!("Failed to get next from server!"),
        None => panic!("Server returned no results!"),
    } 
}
