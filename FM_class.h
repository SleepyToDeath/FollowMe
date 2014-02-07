#include"FM_cfg.h"
/*
class FM_page
{

	public:

	FM_page();
	void add_front(FM_disobj);
	void add_back(FM_disobj);

	vector<FM_disobj> olist;
};


class FM_disobj
{

	public:

	string content;
	style_type style0;

};
*/
class FM_cmd
{
	int type1;
	int type2;
	string content;
};


class FM_data
{

	public:
	data_type dtype;

};


class FM_user: public FM_data
{

	public:

	FM_user();
	index_t following(index_t);	//if following return index in plus else return -1
	index_t followed(index_t);	//the same as above
	index_t published(index_t);		//the same as above
	void add_followed(index_t);	//	TODO 	should keep order
	void add_following(index_t);
	void add_news(index_t);


	index_t id;
	string name;
	string gender;
	string birthday;
	string home;
	string phonenum;

	//	TODO should keep in order or set is considered
	vector <index_t> following;	//those who this guy follows 
	vector <index_t> followed;	//those who follows this guy
	vector <index_t> news;

};

class FM_news: public FM_data 
{

	public:

	FM_news(string); // TODO should set plus_count to zero

	index_t id;
	index_t publisher; //should create an identical entry for each publisher
	index_t share_count; 
	index_t source;	//judge if two news are same by source
	string content;

}



