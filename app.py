from main import *

if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()
    #create_table_expencesgroup(cur)
    #create_expencesgroup(cur, "esdevop", "Pingo_Doce_Bank")
    #create_table_source(cur)
    #cur.execute("DROP TABLE users")
    #create_table_appuser(cur)
    #create_source(cur, "esdevop", "Bank Caixa")
    #create_expencesgroup(cur, "esdevop", "Everyday food")
    #create_table_expences(cur)
    #"""
    create_expences(
        cur,
        login="esdevop",
        expences_name="Pingo doce",
        expences_value=43.08,
        expences_date="2021-01-03",
        expGroup_name="Everyday food",
        source_name="Bank Caixa"
    )
    #"""
    conn.commit()
    #conn.close()