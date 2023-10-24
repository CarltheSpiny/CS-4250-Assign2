#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/
import sys

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
import string
from psycopg2.extras import RealDictCursor
# --> add your Python code here

def connectDataBase():
    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_PORT = "5432"
    DB_HOST = "localhost"
    print("Connecting...")
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn
    except:
        print("Database connection unsuccessful.")
    print("Connection successful.")

def createCategory(cur, cat_id, cat_name):
    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into Category (cat_id, cat_name) Values (%s, %s)"
    recset = [cat_id, cat_name]
    cur.execute(sql, recset)

    print(getCategory(cur, cat_id))

def getCategory(cur, cat_id):
    cur.execute("select * from category where cat_id = %(cat_id)s",
                {'cat_id': cat_id})
    recset = cur.fetchall()

    if recset:
        return str(recset[0]['cat_id'])
    else:
        return ""

def createDocument(cur, id, text, title, date, category):
    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    existingID = getCategory(cur, id)
    if existingID == "":
        return

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    sql = "Insert into documents (id, text, title, date, category) Values (%s, %s, %s, %s, %s)"
    recset = [id, text, title, date, category]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember
    # to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here

    words = text.split()

    for x in words:
        # remove punct
        translator = str.maketrans('', '', string.punctuation)
        cleaned_string = x.translate(translator)
        cleaned_string.lower()

        cur.execute("select term from document_terms where term = %(cleaned_string)s",
               {'cleaned_string}': cleaned_string})
        recset = cur.fetchall()

        cur.execute("select id from document_terms group by id asc limit 1")
        recset2 = cur.fetchall()


        nextID = recset2[0]['id']
        nextID += 1

        count = 0
        for w in words:
            translator = str.maketrans('', '', string.punctuation)
            cleaned_string2 = w.translate(translator)
            cleaned_string2.lower()

            if  cleaned_string2 == cleaned_string:
                count += 1


        sql = "Insert into document_terms (nextID, cleaned_string, id, count) Values (%s, %s, %s, %s)"
        recset3 = [nextID, cleaned_string, id, count]
        cur.execute(sql, recset3)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
    #Answer: included above in document_terms insertion

def deleteDocument(cur, id):
    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # This gets all terms where their doc_id is = id
    cur.execute("select * from document_terms where document_id = %(id)s",
                {'id': id})
    recset = cur.fetchall()

    #This deletes all terms from above
    sql = "Delete from document_terms where document_id = %(id)s"
    cur.execute(sql, {'id': id})

    #Delete terms that don't have this doc_id but also exist in othe docs
    for termIterator in range(len(recset)):
        adjustTerm = recset[termIterator]["term"]
        sql = "Delete from document_terms where term like %(adjustTerm)s"
        cur.execute(sql, {'adjsutTerm': '%[]%'.format(adjustTerm)})


    # 2 Delete the document from the database
    # --> add your Python code here
    sql = "Delete from documents where id = %(id)s"
    cur.execute(sql, {'id': id})

def updateDocument(cur, id, text, title, date, category):
    # 1 Delete the document
    # --> add your Python code here
    sql = "Delete from documents where id = %(id)s"
    cur.execute(sql, {'id': id})

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, id, text, title, date, category)

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    sql = ("select term, title, count(*)"
           "from document_terms, documents"
           "where documents.id = document_terms.id")
    cur.execute(sql)
    recset = cur.fetchall()

    if recset:
        for index in range(len(recset)):
            print(str(recset[index]["term"] + " : " + recset[index]["title"] + ":" + recset['count']))
            if index > 0:
                print(":")
    else:
        return []

    def createTables(cur, conn):
        try:
            sql = ("create table category (cat_id integer not null, cat_name character varying(255) not null, "
                   "constraint cat_id_pk primary key (id))")

            sql = ("create table documents (document_id integer not null, text character varying(255) not null, "
                   "title character varying(255) null, num_chars integer null, date character varying(256), "
                   "category character varying(256), "
                   "constraint docu_id_pkey primary key (id), constraint cate-to-cate-fkey, (category) refrences category (cat_name")
            cur.execute(sql)

            cur.execute(sql)

            sql = ("create table terms (term character varying(255) not null, num_chars integer not null, "
                   "constraint term_pk primary key (id))")
            cur.execute(sql)

            sql = ("create table document_terms (id integer not null, term character varying(256) not null, "
                   "document_id integer not null"
                   "constraint term_pk primary key (id), constraint doc_to_term_fkey, (document_id) references documents (id)")
            cur.execute(sql)

            conn.commit()

        except:
            return