import lib.final_db

final_db = final_db()
cleaned = final_db.cleanup()
final_db.save_db(cleaned)