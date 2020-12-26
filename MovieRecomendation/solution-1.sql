create table users(userid integer primary key, name text);
create table movies (movieid integer primary key, title text);
create table taginfo (tagid integer primary key, content text);
create table genres (genreid integer primary key, name text);
create table ratings (userid integer not null, movieid integer not null,rating numeric check(rating>=0 and rating <=5),timestamp bigint, primary key(userid, movieid), foreign key (userid) references users(userid), foreign key (movieid) references movies (movieid));
create table tags (userid integer not null, movieid integer not null, tagid integer not null , timestamp bigint, primary key(userid, movieid, tagid), foreign key (userid) references users(userid), foreign key (movieid) references movies (movieid), foreign key (tagid) references taginfo (tagid));
create table hasagenre(movieid integer, genreid integer, primary key(movieid, genreid), foreign key (movieid) references movies (movieid), foreign key (genreid) references genres (genreid));
