
pub struct Config {
    pub primitives: HashMap<String, String>;
    pub types: Vec<Type>;
    pub kinds: HashMap<String, Entry>;
}

enum Entry {
    Kind(Kind),
    Type(Type),
    KindAndType(Kind, Type),
}

struct Kind {
    name: String,
}

struct Type {
    name: String,
    args: Value,
}

struct Generator {
    config: Config,
}

impl Generator {
    pub fn new(config: Config) -> Self {

    }
}

fn parse_from_str (input: ) {

}
