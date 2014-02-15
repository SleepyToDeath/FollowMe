#ifndef FM_DATABASE
#define FM_DATABASE

#include<vector>
#include<fstream>

namespace FM_datatbase
{

#define index_t long long

const index_t default_cache_size = 10000000;
const index_t default_cache_capacity = 5000000;

class db_meta
{
	public:

	index_t cache_size;
	index_t cache_capacity;
	index_t block_size;

};

class key
{
	public:

	std::string key;
	index_t index;
};

bool operator > ( key a , key b );

class table_meta 
{
	public:

	index_t entry_len;
	index_t entry_num;
	index_t free_head;
	index_t key_num;
	index_t central_key;
	std::vector<index_t> key_len;
	index_t max_order;

};

class entry
{
	public:

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
	index_t count;

};

class cache_entry
{
	public:

	entry entry_0;
	index_t next;
};

class heap_entry
{
	public:

	heap_entry():key(""),count(0){}
	heap_entry( std::string key_0 , index_t count_0 ):key(key_0),count(count_0){}

	std::string key;
	index_t count;

};


class table
{
	public:

	table(){}

	std::fstream dataio;
	std::vector< std::fstream* > keyios;

	hash<index_t,cache_entry> cache;
	std::vector<heap_entry> heap; 
	hash<std::string,index_t> hash_0; // key , pos

	table_meta table_meta_0;
	std::vector<key_meta> key_metas;

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

	database();
	~database();

/* TODO */

	index_t new_table( index_t len );
	index_t init_table( index_t handler );

	index_t del( index_t handler , index_t index );
	index_t add( index_t handler , char* value , std::vector<std::string> keys=std::vector<std::string>() );
	std::string get( index_t handler , index_t index );

	index_t add_key( index_t handler , index_t len , bool central );
	void get_index( index_t handler , index_t key_handler , std::string key );
	index_t search( index_t handler );
	
	private:

	/* helper functions */

	/* TODO build a tree */
//	std::string get_key( index_t handler , index_t key_handler , index_t index );
//	void ins_key( index_t handler , index_t key_handler , index_t index , index_t order , std::string value );
	
	/* TODO */
	index_t write_data( index_t handler , entry entry_0 ); // return position

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
	std::string store_directory="database/";
	const index_t max_name_len=100;
	db_meta db_meta_0;

	/* variable */

	std::vector<table*> tables;


};

class Btree
{
	public:
	class carrier
	{
		public:
		carrier( index_t pos_0 , index_t rank_0 , std::string key_0 , index_t index_0 );
		
		private:
		index_t pos;
		index_t rank;
		std::string key;
		index_t index;
	};

	Btree( std::string index , index_t cache_size = default_cache_size , index_t cache_capacity = default_cache_capacity );
	void add( std::string key , index_t index );
	void del( std::string key );
	void modify( key_t key , index new_value );

	private:

	class node
	{
		public:

		index_t key_num;
		index_t parent;
		index_t next;
		vector<key> keys;
		vector<index_t> sons; // <position>

	};

	node& accessor( index_t pos );

	index_t cache_capacity;
	index_t cache_size;

	hash<index_t, node> cache; // <position,node>



};


};

#endif
