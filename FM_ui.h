#ifndef FM_UI
#define FM_UI
#include"FM_server.h"

class FM_ui
{

	public:

    FM_ui( bool creative = false );
    ~FM_ui(){ delete server; }
    void run();

    private:

    int signin();   //  1
    int signup();   //  2
    int modify();   //  3
    int search();   //  4
    int follow_list();  //  5
    int news_list();    //  6
    int publish();  //  7
    int follow();   //  8
    int share();    //  9

    int page = 1;

    FM_server* server;
	
};


#endif
