import pyodbc
import datetime

class DatabaseConnection():

    def __init__(self, server, database, user, password):
        #SQL Server 
        """Use this for Azure SQL
        r'DRIVER={SQL Server};'
        """
        self.conn = pyodbc.connect(
            r'DRIVER={ODBC Driver 13 for SQL Server};'
            r'SERVER=' + server + ';'
            r'DATABASE=' + database + ';'
            r'UID=' + user + ';'
            r'PWD=' + password + ';',autocommit=True
            )
        self.conn.execute("set transaction isolation level read uncommitted;")
    
    def insertPage(self, page):
        if(not page):
            return
        cursor = self.conn.cursor()
        cursor.executemany('EXECUTE SaveFacebookPage ?,?,?,?',page)
        cursor.commit()
    
    def insertPosts(self, posts):
        if(not posts):
            return
        cursor = self.conn.cursor()
        cursor.executemany('EXECUTE SaveFacebookPost ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?', posts)
        cursor.commit()

    def insertComments(self, comments):
        if(not comments):
            return
        cursor = self.conn.cursor()
        cursor.executemany('EXECUTE SaveFacebookComment ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?', comments)
        cursor.commit()

    def insertReactions(self, reactions):
        if(not reactions):
            return
        cursor = self.conn.cursor()
        cursor.executemany('EXECUTE SaveFacebookReaction ?,?,?,?', reactions)
        cursor.commit()

    def closeConnection(self):
        self.conn.close()
        