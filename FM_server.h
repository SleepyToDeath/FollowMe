#ifndef FM_SERVER
#define FM_SERVER

#include"FM_database.h"


class FM_user
{
    public:

    std::string value();
    std::vector<std::string> keys();
    FM_user();
    FM_user( std::string value );

    const static int usernamel = 16;
    const static int passwordl = 16;
    const static int namel = 32;
    const static int genderl = 6;
    const static int birthdayl = 12;
    const static int homel = 32;
    const static int phonenuml = 16;
    const static int len = usernamel
            + passwordl
            + namel
            + genderl
            + birthdayl
            + homel
            + phonenuml;

    std::string username = std::string(' ',usernamel);    // key 0
    std::string password = std::string(' ',passwordl);
    std::string name = std::string(' ',namel);            // key 1
    std::string gender = std::string(' ',genderl);
    std::string birthday = std::string(' ',birthdayl);    // key 2
    std::string home = std::string(' ',homel);            // key 3
    std::string phonenum = std::string(' ',phonenuml);

};

class FM_news
{

    public:

    std::string value();
    std::vector<std::string> keys();
    FM_news();
    FM_news( std::string value );

    const static int news_len = 140;
    const static int len = news_len + 3*sizeof(index_t);

    index_t publisher = -1; //should create an identical entry for each publisher   // key 0
    index_t source = -1;	//judge if two news are same by source
    index_t order = -1;

    std::string news;

};

class FM_relation
{

    public:

    const static int len = 2*sizeof(index_t);

    std::string value();
    std::vector<std::string> keys();
    FM_relation();
    FM_relation( std::string value );

    index_t following = -1; // key 0
    index_t followed = -1;  // key 1

};

class FM_server
{
	public:

    FM_server( bool creative=false );
    ~FM_server();

    bool signup( FM_user new_user );
    bool signin( std::string username , std::string password );
    bool modify( FM_user new_user ); // change info & password
    bool follow( index_t followed );
    bool unfollow( index_t followed );
    std::vector< std::pair<index_t,FM_user> > search( std::string key , std::string key_1 , std::string key_2 );   // after that , each time this is called , an user is returned
    std::vector< std::pair<index_t,FM_user> > follow_list();     // the same as search
    bool refresh();
    FM_news news_list();
    bool post( FM_news news );
    bool share( index_t index );
    FM_user get_user();
    index_t get_id();
    std::string get_name( index_t id );

    private:

    FM_database::database* db;

    index_t id = -1;
    FM_user user;

    size_t cur_news;
    std::vector<index_t> followeds;
    std::vector<FM_database::Btree::carrier*> carriers;
    std::vector<FM_news> news;

    index_t userh;
    index_t usernamekh;
    index_t namekh;
    index_t birthdaykh;
    index_t homekh;

    index_t newsh;
    index_t publisherkh;

    index_t relationh;
    index_t followingkh;
    index_t followedkh;

};

#endif
