import lib.final_db as fdb

final_db = fdb.final_db()
cleaned = final_db.cleanup()
final_db.save_db(cleaned)