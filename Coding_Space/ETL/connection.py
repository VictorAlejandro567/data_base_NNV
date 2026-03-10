import psycopg2

def get_connection():
    conn = psycopg2.connect(
      host="dpg-d6m51rc50q8c73ad434g-a.virginia-postgres.render.com",
      database="dbclass_j9l7",
      user="dbclass_j9l7_user",
      password="fxG0fOxukbJIK8wZScUSxT3fNKcEOVCO",
      port=5432
    )
    return conn