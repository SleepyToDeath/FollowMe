#include"FM_database.h"
#include<sstream>

using namespace FM_datatbase;
using std::string;
using std::vector;
using std::stringstream;
using std::fstream;
using std::ios;

index_t max( index_t a , index_t b )
{
	if ( a > b ) return a;
	else return b;
}

index_t atoi( string s )
{
	int l=s.length();
	index_t i=0;
	int j=0;
	while ( s[j]==' ' || s[j]=='\t' )
		j++;
	for (;j<l;j++)
		i=i*10+s[j]-'0';
	return i;
}

string itos( index_t i )
{
	stringstream ss;
	ss<<i;
	return ss.str();
}

string iexts( index_t i )
{
	int l = sizeof( index_t );
	string tmp="";
	for (int j=0; j<l; j++)
		tmp+=(char)(i>>(j*8));
	return tmp;
}

bool operator > ( key a , key b )
{
	return ( a.key > b.key || a.key == b.key && a.index < b.index );
}

database::database()
{
	fstream fin(init_file_name);
	string s;
	while (getline(fin,s))
	{
		if (s.find("cache_size")) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_capacity=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("block_size")) 
		{
			size_t pos=s.find("=");
			db_meta_0.block_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
	}

}

/* table & cache */

index_t database::new_table( index_t len )
{
	table* table_0=new table;
	tables.push_back(table_0);
	table_0->dataio.open((itos(tables.size()-1)+".dat").c_str() , ios::binary | ios::in | ios::out );
	fstream metaio((itos(tables.size()-1)+".meta").c_str() , ios::binary | ios::in | ios::out );

}


index_t database::add( index_t handler , char* value , std::vector<std::string> keys )
{
	table* table_0=tables[ handler ];

	entry tmp;
	tmp.keys = keys;
	tmp.value = string( value );
	tmp.index = table_0->table_meta_0.max_order;
	table_0->table_meta_0.max_order++;
	tmp.keys.push_back( iexts(tmp.index) );

	cache_entry tmp_2;
	tmp_2.entry_0 = tmp;
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	if ( table_0->hash_0.find( tmp_ck ) )
	{
		tmp_2.next = table_0->hash_0[ tmp_ck ];
		table_0->hash_0[ tmp_ck ] = tmp.index;
		heap_inc( handler , tmp_ck , 1 );
	}
	else
	{
		table_0->hash_0.add( tmp_ck , tmp.index );
		heap_add( handler , tmp_ck , 1 );
		tmp_2.next = -1;
	}
	table_0->cache.add( tmp.index , tmp_2 );

	if ( table_0->cache.count() >= db_meta_0.cache_capacity )
	{
		index_t i = table_0->hash_0[ heap_min( handler ) ];
		while ( i >= 0 )
		{
			tmp_2 = table_0->cache[i];
			index_t tmp_i = write_data( handler , tmp_2.entry_0 );
			/* TODO change pos in index key tree */

			table_0->cache.del( i );
			i = tmp_2.next;
		}
		table_0->hash_0.del( heap_min( handler ) );
		heap_del( handler , 1 );
	}

}

index_t database::del( index_t handler , index_t index )
{
	
}

string get( index_t handler , index_t index )
{


}

/* heap */

void database::swap_heap_entry( index_t handler , index_t i , index_t j )
{
	table* table_0 = tables[handler];
	table_0->hash_0[table_0->heap[i].key] = j;
	table_0->hash_0[table_0->heap[j].key] = i;
	heap_entry tmp = table_0->heap[j];
	table_0->heap[j] = table_0->heap[i];
	table_0->heap[i] = tmp;
}

void database::heap_up( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	index_t i=pos/2;
	if (i>0)
		if ( table_0->heap[i].count > table_0->heap[pos].count )
		{
			swap_heap_entry( handler , pos , i );
			heap_up( handler , i );
		}
}

void database::heap_down( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	index_t i=pos;
	if (pos*2 <= (long long)table_0->heap.size()-1)
	{
		if ( table_0->heap[pos].count > table_0->heap[pos*2].count )
			i=pos*2;
		if ( pos*2+1 <= (long long)table_0->heap.size()-1 &&
			table_0->heap[i].count > table_0->heap[pos*2+1].count )
			i=pos*2+1;
	}
	if ( i>pos )
	{
		swap_heap_entry( handler , pos , i );
		heap_down( handler , i );
	}
}

void database::heap_del( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	swap_heap_entry( handler , pos , table_0->heap.size()-1 );
	table_0->heap.pop_back();
	heap_up( handler , pos );
	heap_down( handler , pos );
}

void database::heap_add( index_t handler , string key , index_t count_0 )
{
	table* table_0=tables[ handler ];
	table_0->heap.push_back( heap_entry( key,count_0 ) );
	table_0->hash_0[ key ] = table_0->heap.size();
	heap_up( handler , table_0->heap.size()-1 );
}

void database::heap_inc( index_t handler , string key , index_t delta )
{
	table* table_0=tables[ handler ];
	table_0->heap[ table_0->hash_0[ key ] ].count += delta;
}

string database::heap_min( index_t handler )
{
	return tables[ handler ]->heap[1].key;
}

/* hash */

template<class key_t , class value_t> 
hash<key_t,value_t>::hash( index_t size )
{
	vector<bool> prime( false , size+1 );
	for (int i=2; i*i<=size; i++)
	{
		if ( !prime[i] )
			for (int j=1;i*j<=size;j++)
				prime[i*j]=true;
	}
	for (int j=size; j>1; j--)
		if ( !prime[j] )
		{
			prime_0=j;
			break;
		}
	table=vector<hash_table_entry>( hash_table_entry() , size+1 );
	counter=0;

}

template<class key_t , class value_t>
void hash<key_t,value_t>::add( key_t key , value_t value )
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used || !table[hash_value].valid || table[hash_value].key==key)
		{
			table[hash_value].used=true;
			table[hash_value].valid=true;
			table[hash_value].key=key;
			table[hash_value].value=value;
			counter--;
			break;
		}
	}

}

template<class key_t , class value_t>
void hash<key_t,value_t>::del( key_t key )
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (table[hash_value].used && table[hash_value].valid && table[hash_value].key==key)
		{
			table[hash_value].valid=false;
			counter--;
		}
	}
}

template<class key_t , class value_t>
value_t& hash<key_t,value_t>::operator[](key_t key)
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used || table[hash_value].valid && table[hash_value].key==key)
			return table[hash_value].value;
	}
}

template<class key_t , class value_t>
bool hash<key_t,value_t>::find(key_t key)
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used)
			return false;
		else if ( table[hash_value].valid && table[hash_value].key==key)
			return true;
	}
	return false;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::count()
{
	return counter;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::h0( index_t value )
{
	return value%prime_0;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::h0( string value ) // BKDR hash
{
	index_t hash=0;
	index_t seed=131313; //strange hm......
	int l=value.length();
	for (int i=0;i<l;i++)
		hash = hash * seed + value[i];

	return hash % prime_0;
}

template<class key_t , class value_t>
index_t hash<key_t,value_t>::h( key_t key , index_t i )
{
	index_t key_1=h0(key);
	return ( key_1 % prime_0 + i * key_1 % (prime_0-1) ) % prime_0;
}

Btree::carrier::carrier( index_t pos_0 , index_t rank_0 , std::string key_1_0 , std::string key_2_0 , index_t index_0 )
:pos(pos_0),rank(rank_0),key_1(key_1_0),key_2(key_2_0),index(index_0)
{
}

index_t Btree::node::find( key key_0 )
{
	int i=0, j=key_num-1;
	while ( i<j )
	{
		k = (i+j)/2;
		if ( key_0 > keys[k] )
			i = k+1;
		else
			j = k;
	}
	return i-1;
}

Btree::Btree( std::string index_0 , index_t cache_size_0 , index_t cache_capacity_0 , index_t node_size_0 , index_t key_size_0 )
{
	meta.index = index_0;
	meta.cache_size = cache_size_0;
	meta.cache_capacity = cache_capacity_0;
	meta.node_size = node_size_0;
	meta.key_size = key_size_0;
	meta.root = -1;
	meta.max_pos = -1;
	meta.free_head = -1;
	meta.height = -1;
	meta.node_size = node_size/(meta.key_size+sizeof(index_t)*2);
	cache = hash<index_t, node>( meta.cache_size );
}

Btree::Btree( std::string index_0 )
{
	fstream fin((index+".meta").c_str());
	string s;
	while ( getline(fin,s) )
	{
		if (s.find("cache_size")) 
		{
			size_t pos = s.find("=");
			meta.cache_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")) 
		{
			size_t pos = s.find("=");
			meta.cache_capacity = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_size")) 
		{
			size_t pos = s.find("=");
			meta.key_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("node_size")) 
		{
			size_t pos = s.find("=");
			meta.node_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("root")) 
		{
			size_t pos = s.find("=");
			meta.root = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_pos")) 
		{
			size_t pos = s.find("=");
			meta.max_pos = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("free_head")) 
		{
			size_t pos = s.find("=");
			meta.free_head = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("heght")) 
		{
			size_t pos = s.find("=");
			meta.height = atoi(s.substr(pos+1,s.length()-pos-1));
		}
	}

	meta.node_size = node_size/(meta.key_size+sizeof(index_t)*2);
	cache = hash<index_t, node>( meta.cache_size );
}

void Btree::add( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key;
	tmp.index = index_0;

	if ( root == -1 )
	{
		root = new_node();	
		height = 0;
	}
	index_t cur = root;
	for (int i=0; i<meta.height; i++)
	{
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		if ( tmpi>=0 && tmpn.keys[tmpi]==key_0 ) return;
		cur = tmpn.sons[tmpi+1];
	}

	node& tmpn = accessor( cur );
	int tmpi = tmpn.find( tmp );
	if ( tmpi>=0 && tmpn.keys[tmpi]==key_0 ) return;
	index_t new_son = -1;
	while ( cur>=0 )
	{
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		for (int i=tmpn.key_num-1; i>tmpi; i--)
		{
			tmpn.keys[i+1] = tmpn.keys[i];
			tmpn.sons[i+2] = tmpn.sons[i+1];
		}
		tmpn.keys[tmpi+1] = tmp;
		tmpn.sons[tmpi+2] = new_son;
		if ( tmpn.key_num < meta.key_size-1 ) break;
		tmpi = tmpn.key_num / 2;
		new_son = new_node();
		node& tmpn2 = accessor( new_son );
		for (int i=tmpi+1; i< tmpn.key_num; i++)
		{
			tmpn2.keys[i-tmpi-1] = tmpn.keys[i];
			tmpn2.sons[i-tmpi-1] = tmpn.sons[i];
		}
		tmpn2.sons[tmpn.key_num-tmpi] = tmpn.sons[tmpn.key_num];
		tmp = tmpn.keys[tmpi];
		tmpn.key_num = tmpi;
		tmpn2.key_num = key_num-tmpi-1;
		tmpn2.parent = tmpn.parent;
		cur = tmpn.parent;
	}
	if ( cur == -1 )
	{
		index_t tmpi = new_node();
		node& tmpn = accessor( tmpi );
		tmpn.keys[0] = tmp;
		tmpn.sons[0] = root;
		tmpn.sons[1] = new_son;
		accessor( root ).parent = tmpi;
		root = tmpi;
	}
		
}

void Btree::del( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key_0;
	tmp.index = index_0;
	index_t cur = root;
	int tmpi = -1;
	while ( cur>=0 )
	{
		node& tmpn = accessor( cur );
		tmpi = tmpn.find( tmp );
		if ( tmpn.keys[tmpi] == tmp ) break;
		cur = tmpn.sons[tmpi+1];
	}
	// now cur & tmpi is what I need
	if ( cur == -1 ) return;
	node& tmpn = accessor( cur );
	if ( tmpn.sons[tmpi+1] == -1 )
	{
		for (int i=tmpi; i<tmpn.key_num-1; i++)
			tmpn.keys[i] = tmpn.keys[i+1];
		tmpn.key_num--;
	}
	else
	{
		index_t tmpi2 = tmpn.sons[tmpi];
		index_t tmpi3;
		while ( tmpi2>=0 )
		{
			tmpi3 = tmpi2;
			node& tmpn2 = accessor( tmpi2 );
			tmpi2 = tmpn2.sons[tmpn2.key_num-1];
		}
		node& tmpn2 = accessor( tmpi2 );
		tmpn.keys[tmpi] = tmpn2.keys[tmpn2.key_num-1];
		tmpn2.key_num--;
		cur = tmpi2;
	}

	//now adjust cur
	if ( cur == root || accessor( cur ).key_num >= (meta.node_size+1)/2-1 ) return;
	index_t tmpib;
	node& tmpn = accessor( cur );
	node& tmpnp = accessor( tmpn.parent );
	tmpi = tmpn.find( tmpn.keys[0] );
	if ( tmpi == -1 )
		tmpib = tmpnp.sons[1];
	else
		tmpib = tmpnp.sons[tmpi]
	node& tmpnb = accessor( tmpib );
	if ( tmpnb.key_num > (meta.node_size+1)/2-1 )
	{
		if ( tmpi >= 0 )
		{
			for (int i=tmpn.key_num; i>0; i--)
			{
				tmpn.keys[i]=tmpn.keys[i-1];
				tmpn.sons[i+1]=tmpn.sons[i];
			}
			tmpn.sons[1] = tmpn.sons[0];
			tmpn.keys[0] = tmpnp.keys[ tmpi ];
			tmpn.sons[0] = tmpnb.sons[ tmpnb.key_num ];
			tmpnp.keys[ tmpi ] = tmpnb.keys[ tmpnb.key_num-1 ];
			tmpnb.key_num--;
			tmpn.key_num++;
		}
		else
		{
			tmpn.keys[ tmpn.key_num ] = tmpnp.keys[0];
			tmpn.sons[ tmpn.key_num+1 ] = tmpnb.sons[0];
			tmpnp.keys[0] = tmpnb.keys[0];
			for (int i=0; i<tmpnb.key_num-1; i++)
			{
				tmpnb.keys[i]=tmpnb[i+1];
				tmpnb.sons[i]=tmpnb[i+1];
			}
			tmpnb.sons[ tmpnb.key_num-1 ] = tmpnb.sons[ tmpnb.key_num ];
			tmpn.key_num++;
			tmpnb.key_num++;
		}
	}
	else
	{
		// TODO combination
		
		while (true)
		{


		}
	}







}











	

