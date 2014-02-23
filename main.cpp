#include"FM_database.h"
#include<iostream>
using namespace FM_database;
using std::string;
using std::vector;
using std::endl;
using std::cout;

void basic()
{
	database db( true );
	index_t h0 = db.new_table( 5 );
	index_t k0 = db.add_key( h0 , 5 , true );
	db.init_table( h0 );

	std::string s1 = "12345";
	vector<string> k1;
	k1.push_back("s1k11");

	std::string s2 = "67890";
	vector<string> k2;
	k2.push_back("s2k21");

	index_t i1 = db.add( h0 , s1 , k1 );
	index_t i2 = db.add( h0 , s2 , k2 );

	Btree::carrier* c1 = db.search( h0 , k0 , k1[0] , k1[0] );
	Btree::carrier* c2 = db.search( h0 , k0 , k2[0] , k2[0] );
	std::cout<<"i1="<<i1<<" found one is "<<c1->next()<<std::endl;
	std::cout<<"i2="<<i2<<" found one is "<<c2->next()<<std::endl;
	cout<<db.get( h0 , i1 )<<endl;
	cout<<db.get( h0 , i2 )<<endl;

	cout<<"Basic is over"<<endl;
}

void small()
{
	int n = 100;
	database db( true );
	index_t h1 = db.new_table( 5 );
	index_t k1 = db.add_key( h1 , 5 , true );
	db.init_table( h1 );
	vector<string> v1;
	vector< vector<string> > v2;
	for (int i=0; i<n; i++)
	{
		v1.push_back( itos( i ) );
		v2.push_back( vector<string>( 1 , "k"+itos( i%20 ) ) );
	}
	for (int i=0; i<n; i++)
		db.add( h1 , v1[i] , v2[i] );
	vector<Btree::carrier*> v3;
	for (int i=0; i<20; i++)
		v3.push_back( db.search( h1 , k1 , "k"+itos( i ) , "k"+itos( i ) ) );
	for (int i=0; i<20; i++)
	{
		cout<<'k'<<i<<" :"<<endl;
		index_t tmpi;
		tmpi = v3[i]->next();
		while (tmpi>=0)
		{
			cout<<db.get( h1 , tmpi )<<' ';
			tmpi = v3[i]->next();
		}
		cout<<endl;
	}

}

int main()
{
	small();
}
