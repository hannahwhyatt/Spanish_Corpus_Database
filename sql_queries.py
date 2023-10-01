

# DROP TABLE

def drop_table_query(table):
    return 'DROP TABLE IF EXISTS {}'.format(table)

# CREATE TABLE

create_main_table = '''
        CREATE TABLE IF NOT EXISTS main_table
        (
            textID INT,
            seqID BIGSERIAL,
            wID INT
        )
        '''

create_lexicon_table = '''
        CREATE TABLE IF NOT EXISTS lexicon
        (
            wID INT PRIMARY KEY,
            word VARCHAR(50),
            lemma VARCHAR(50),
            pos VARCHAR(15)
        )
        '''
create_source_table = '''
        CREATE TABLE IF NOT EXISTS text_source
        (
            textID INT PRIMARY KEY,
            nwords INT,
            genre VARCHAR(1),
            country VARCHAR(2)
        )
        '''

# INSERT INTO

insert_main_table = '''
    INSERT INTO main_table
    (textID, seqID, wID)
    VALUES (%s, %s, %s)
    '''

insert_lexicon_table = '''
    INSERT INTO lexicon
    (wID, word, lemma, pos)
    VALUES (%s, %s, %s, %s)
    '''

insert_source_table = '''
    INSERT INTO text_source
    (textID, nwords, genre, country)
    VALUES (%s, %s, %s, %s)
    '''