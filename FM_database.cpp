#include"FM_database.h"

database::~database()
{
	fstream fin(init_file_name);
	string s;
	getline(fin,s);
	if (s.find("cache_size")) 
	{
		size_t pos=s.find("=");
		cache_size=atoi(s.substr(pos+1,s.length()-pos-1));
	}

	

	init_hash();
}

index_t database::new_table( index_t len );
{
	table* table_0=new table;
	tables.push_back(table_0);
	table_0.dataio.open(itoa(tables.size(),nullptr,10)+".dat" , ios::binary | ios::in | ios::out );
	fstream metaio(itoa(tables.size(),nullptr,10)+".meta" , ios::binary | ios::in | ios::out );





}


index_t database::add( index_t handler , char* value )
{
	store* store_0=tables[handler];
}

index_t database::del( index_t handler , index_t index )
{
	
}

string get( index handler , index_t index );
{

}

void database::init_hash()
{
	vector<bool> prime(false,cache_size+1);
	memset(prime,0,sizeof(prime));
	for (int i=2;i*i<=cache_size;i++)
	{
		if (!prime[i])
			for (j=1;i*j<=cache_size;j++)
				prime[i*j]=true;
	}
	for (int j=cache_size;j>1;j--)
		if (!prime[j])
		{
			prime_0=j;
			break;
		}

}

