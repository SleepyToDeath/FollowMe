#ifndef FM_DATABASE
#define FM_DATABASE

#include<vector>
#include<queue>
#include<fstream>
#include<climits>

namespace FM_database
{

#define index_t long long
#define MAX_INDEX_T LLONG_MAX

const index_t default_cache_size = 100000;
const index_t default_cache_capacity = 50000;
const std::string store_directory="database/";

class key;

index_t max( index_t a , index_t b );

index_t abs( index_t i );

std::string mend_string( std::string s , std::size_t len );

index_t atoi( std::string s );

std::string itos( index_t i );

std::string iexts( index_t i );

void put_int( std::fstream& fio , index_t i );

index_t get_int( std::fstream& fio );

std::string get_string( std::fstream& fio , index_t len );

bool operator == ( key a , key b );

bool operator > ( key a , key b );


class db_meta
{
	public:

	index_t cache_size;
	index_t cache_capacity;
	index_t block_size; // a.k.a. node_size
	index_t recent_range;
	index_t table_num;

};

class key_meta
{
	public:

	std::string index;
	index_t cache_capacity;
	index_t cache_size;
	index_t node_size;
	index_t key_size;
	index_t max_pos;
	index_t free_head;
	index_t root;
	index_t height;

};

class table_meta 
{
	public:

	index_t entry_size;
	index_t value_size;
	index_t max_entry_num; // max_pos/entry_size  initially 0
	index_t free_head;
	index_t key_num;
	index_t central_key;
	std::vector<index_t> key_len;
	index_t max_order;

};


class key
{
	public:

	std::string key;
	index_t index;
};

//bool operator > ( key a , key b );

class entry
{
	public:
	entry(){}
	entry( const entry& tmpe ):valid( tmpe.valid ),index( tmpe.index ),pos( tmpe.pos ),value( tmpe.value ),keys( tmpe.keys ){}

	char valid;
	index_t index; // order
	index_t pos;
	std::string value;
	std::vector<std::string> keys;
};


template<class key_t , class value_t>
class hash
{

	public:

	hash( index_t size );
	hash(){}
	void add( key_t key , value_t value );
	void del( key_t key );
	bool find( key_t key );
	index_t count();
	value_t& operator[]( key_t key );

	private:

//	template<class key_t , class value_t>
	class hash_table_entry
	{
		public:

		hash_table_entry():valid(false),used(false){}
		hash_table_entry( const hash_table_entry& tmphte ):valid( tmphte.valid ),used( tmphte.used ),key( tmphte.key ), value( tmphte.value ){}

		bool valid;
		bool used;
		key_t key;
		value_t value;
	};

	index_t h0( index_t value );
	index_t h0( std::string value );
	index_t h( key_t key , index_t i );

	std::vector<hash_table_entry> table;

    index_t prime_0;
    index_t prime_1;
	index_t counter;

};

class cache_entry
{
	public:
	cache_entry(){}
	cache_entry( const cache_entry& tmpce ):entry_0( tmpce.entry_0 ),next( tmpce.next ),prev(tmpce.prev) {}
	entry entry_0;
	index_t next , prev;
};

class heap_entry
{
	public:

	heap_entry():key(""),count(0){}
	heap_entry( std::string key_0 , index_t count_0 ):key(key_0),count(count_0){}

	std::string key;
	index_t count;

};

/* a simple B-tree...... with a cache
 * elements are ranked in rising order
 * keys with bigger indices , which means they appear latter , are arranged first
 */
class Btree
{
	friend class carrier;

	public:

	class carrier
	{
		public:
		carrier( std::string key_1_0 , std::string key_2_0 , index_t index_0 , Btree* tree_0 );
		index_t next(); // return next matching index ; -1 if invalid
		
		private:
		index_t pos;
		index_t rank;
		std::string key_1 , key_2 , key_0;
		index_t index;
		Btree* tree;
		bool initial;
	};

	Btree( std::string index_0 , index_t cache_size_0 = default_cache_size , index_t cache_capacity_0 = default_cache_capacity , index_t node_size_0 = default_node_size , index_t key_size_0 = 0 );
	Btree( std::string index_0 , bool resume ); // read from meta file
	~Btree();

	void add( std::string key , index_t index );
	void del( std::string key , index_t index );

    index_t count( index_t cur = -2 );
	/* modify is a fast way to change a value. BUT , 
	 * make sure each key has a single corresponding value otherwise the behaviour is undefined
	 * normally you should use del & add to change a value
	 */
	void modify( std::string key , index_t new_value ); 
	carrier* search( std::string key_1 , std::string key_2 );	//return all indices in the range 
    index_t search( std::string key );	//return the first one matching the given key
//  why did I do this ? insane?					 //	index_t search( std::string key , index_t index );	//return the exact one matching the given info

	private:

	class node
	{
		public:

		index_t find( key key_0 ); // find the last one <= key_0

		index_t key_num;
		index_t parent;
		index_t next , prev; //not writen to data file
		std::vector<key> keys;
		std::vector<index_t> sons; // <position> ; sons[i] is the subtree before keys[i]

	};

	std::pair<index_t,index_t> inner_search( std::string key ); //find the first one matching the given key , return node pos & rank
	node& accessor( index_t pos );
	index_t new_node();
	void del_node( index_t pos );
	void write_node( index_t pos , node n );
	node read_node( index_t pos );

	/* variable */
	key_meta meta;

	index_t cache_head = -1 , cache_tail = -1; // newest is at the tail
	index_t node_size_byte;

	hash<index_t, node> cache; // <position,node>

	std::fstream fio;

	/* constant */
	const static index_t default_node_size = 512;

};


class table
{
	public:

	table(){}
    ~table(){
        for (int i=0; i<table_meta_0.key_num+1; i++)
            delete keys[i];
    }

	std::fstream fio;
//	std::vector< std::fstream* > keyios;

	hash<index_t,cache_entry> cache;
	std::vector<heap_entry> heap; 
	hash<std::string,std::pair<index_t,index_t> > hash_0; // key , <pos,head>
	std::queue<std::string> recent;

	table_meta table_meta_0;
//	std::vector<key_meta> key_metas;
	std::vector<Btree*> keys;

	bool ready;

};

/*
 * this database is designed for data with limited length
 * on disk everything is a string
 * new_db creates a new data base with a meta & a data file
 * new_db returns a handler>=0 if success ,-1 if fail
 * index (TODO it is better to be position , but there would be problem in cache )(in fact is the order it is added) is the only key to identify an entry
 * add_key will add a key to the data specified by handler, if success, a key handler will
 * be returned, a meta & an index file will be created
 * a B tree will be created for each key, and stored in index file
 * keys are compared by string ( this should be ok if integers are converted to strings )
 * entries are stored in cache by central key ( that is, when writen back, entries with 
 * same central key will be writen back together, and when read from disk, a constant
 * sequential of entries are read together so that if entries with the same central key
 * are often used together, less read would be used )
 * TODO central key could be set only once right now
 * TODO all keys must be set before using right now
 */

class database
{

	public:

	database( bool new_db );
	~database();

/* TODO */

	index_t new_table( index_t len );
	index_t add_key( index_t handler , index_t len , bool central );
	void init_table( index_t handler );

	std::string del( index_t handler , index_t index ); // return the value , "" if fail
	index_t add( index_t handler , std::string value , std::vector<std::string> keys=std::vector<std::string>() );
	std::string get( index_t handler , index_t index );
	Btree::carrier* search( index_t handler , index_t key_handler , std::string key_1_0 , std::string key_2_0 );

//	void get_index( index_t handler , index_t key_handler , std::string key );
//	index_t search( index_t handler );
	
	private:

	/* helper functions */

	/* TODO build a tree */
//	std::string get_key( index_t handler , index_t key_handler , index_t index );
//	void ins_key( index_t handler , index_t key_handler , index_t index , index_t order , std::string value );
	
	/* TODO */
	index_t write_data( index_t handler , entry entry_0 , bool relocate = true ); // return position
	entry read_data( index_t handler , index_t pos , bool relocate = true );

	void add_to_cache( index_t handler , entry tmpe );
	void check_full( index_t handler );
	void del_disk( index_t handler , index_t pos );
	void write_back( index_t handler );

	void resume_table( index_t index );
	void clear_table( index_t handler );

	/* heap */

		void swap_heap_entry( index_t handler , index_t i , index_t j );
		void heap_up( index_t handler , index_t pos );
		void heap_down( index_t handler , index_t pos );
		void heap_add( index_t handler , std::string key , index_t count_0 );
		void heap_del( index_t handler , index_t pos );
		void heap_inc( index_t handler , std::string key , index_t delta );
		std::string heap_min( index_t handler );

	/* constant */

	std::string init_file_name="database.ini";
	const index_t max_name_len=100;
	db_meta db_meta_0;

	/* variable */

	std::vector<table*> tables;


};

};

#endif

