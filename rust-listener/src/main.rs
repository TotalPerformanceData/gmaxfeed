/*

bind to a port and listen for UDP packets sent from gmax servers.

on receipt of a packet, program can either populate a filesystem itself
or pass the data into a message queue such as redis or rabbitmq

parent_thread: init the queue, spawn child_thread, then bind to socket and begin listening.
child_thread: await additions to the queue, if any then perform the function be it adding to
file system or forwarding to redis.

author: George Swindells
email: george.swindells@totalperformancedata.com

*/

use std::env;
use std::thread;
use std::net::UdpSocket;
use std::str::from_utf8;
use std::sync::mpsc::{channel, Sender, Receiver};
use redis;
use redis::{RedisResult, Connection};

static HOST: &'static str = "0.0.0.0";
static PORT: i32 = 33322;


fn handle_with_filesystem() {
    println!("handling with filesystem from within rust");
    // TODO
}


fn handle_with_redis(rx: &Receiver<String>) -> RedisResult<Connection> {
    // handle the string packets received by putting into redis Queue as string
    // can also add a fire and forget PUBSUB
    let redis_password = match env::var("REDIS_PASSWD") {
        Ok(val) => val,
        Err(_e) => String::from("NONE"),
    };
    let redis_url = format!("redis://:{}@127.0.0.1:6379/", redis_password);
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_connection()?; // returns error if not successful

    loop {
        // element in queue, execute the code to deal with it
        let res = match rx.recv() {
               Ok(v) => v,
               Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };
        let _ : () = redis::cmd("LPUSH").arg("test_queue").arg(res).query(&mut conn)?;
    }
}


fn listen(tx: & Sender<String>, port_number:i32) {
    let mut buffer: [u8; 2048] = [0; 2048];
    //let addr = SocketAddr::from((HOST, PORT));
    //let addrs = addr.to_socket_addrs().unwrap();
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", port_number)).expect(&format!("Failed to bind to address {0}:{1}", HOST, port_number));
    println!("Bound to socket on {0}:{1}, listening...", HOST, port_number);
    loop{
        let (amt, _src) = socket.recv_from(&mut buffer).expect("Didn't receive data");
        let _reaction = {
            let msg = match from_utf8(&buffer[0..amt]) {
                    Ok(v) => v,
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
            let s = String::from(msg);
            tx.send(s).unwrap();
        };
    }
}


fn main() {
    println!("Starting Rust Gmax feed listner...");
    // get the port number from the command line argument, if none given then use the global hardcoded val. Program name is arg[0]
    let args: Vec<String> = env::args().collect();
    let mut port_number:i32 = PORT;
    if args.len() > 1 {
        port_number = args[1].parse::<i32>().unwrap();
        println!("given port number as: {}", port_number);
    } else {
        println!("Using default port number: {}", port_number);
    }

    // initialise the inter-thread communication
    let (tx, rx): (Sender<String>, Receiver<String>)  = channel();
    //let test_msg = String::from("Hello, this is a test message");
    //tx.send(test_msg).unwrap();
    // spawn the child thread which performs packet management
    let _child_thread = thread::spawn(move || {
        let _thr = match handle_with_redis(&rx){
            Ok(r) => r,
            Err(error) => panic!("Problem spawning handler: {:?}", error),
        };
    });
    listen(&tx, port_number);
}
