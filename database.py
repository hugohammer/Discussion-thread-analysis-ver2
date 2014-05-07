#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sqlalchemy as sa
from datetime import datetime



def facebook_table_creator(metadata):
    return sa.Table('messages',metadata,\
                    sa.Column('post_id',sa.String(42),nullable=False),\
                    #sa.Column('post_id1',sa.BigInteger,autoincrement=False,primary_key=True),\
                    #sa.Column('post_id2',sa.BigInteger,autoincrement=False,primary_key=True),\
                    #sa.Column('post_id1',sa.BigInteger,nullable=False),\
                    #sa.Column('post_id2',sa.BigInteger,nullable=False),\
                    sa.Column('object_id',sa.String(22)),\
                    sa.Column('message',sa.String(63206)),\
                    sa.Column('story',sa.String(1024)),\
                    sa.Column('post_type',sa.String(32)),\
                    sa.Column('created_time',sa.DateTime),\
                    sa.Column('updated_time',sa.DateTime),\
                    sa.Column('poster_id',sa.String(22)),\
                    sa.Column('poster_name',sa.String(128)),\
                    sa.Column('link',sa.String(1024)),\
                    sa.Column('picture',sa.String(1024)),\
                    sa.Column('icon',sa.String(1024)),\
                    sa.Column('caption',sa.String(1024)),\
                    sa.Column('application',sa.String(1024)),\
                    sa.Column('wall_name',sa.String(128)),\
                    sa.Column('iscomment',sa.Boolean),\
                    sa.Column('parent',sa.String(42)),\
                    sa.Column('like_count',sa.BigInteger),\
                    sa.Column('comment_count',sa.BigInteger),\
                    sa.Column('user_likes',sa.Boolean),\
                    mysql_charset='utf8' )


def get_thread(parent,table):
    sel=table.select().where(table.c['post_id']==parent)
    cur=sel.execute()
    thread=cur.fetchall()
    cur.close()

    sel=table.select()
    sel=sel.where(table.c['parent']==parent)
    sel=sel.order_by(table.c['created_time'])
    cur=sel.execute()
    thread.extend(cur.fetchall())
    cur.close()
    return thread



def main():

    eng = sa.create_engine('sqlite:///databases/facebook_walls.sqlite',echo=False)
    meta=sa.MetaData(eng)

    facebook_table=facebook_table_creator(meta)

    sel=facebook_table.select()
    sel=sel.where(facebook_table.c['wall_name']=='nrk')
    cur=sel.execute()
    
    for iii,post in enumerate(cur):
        print(iii,post['message'])
        print()
        if iii>2:
            break
    cur.close()

    sel = sa.select([facebook_table.c['wall_name'].distinct()])
    cur=sel.execute()
    walls=cur.fetchall()
    print('Walls:',walls)
    print()
    cur.close()

    thread=get_thread('227055917332677_670420762996188',facebook_table)
    for iii,row in enumerate(thread):
        print(iii,row['message'],'\n','Poster:',row['poster_name'],row['created_time'])
        print()
  
    return
    
    
if __name__ == '__main__': 
    main()
