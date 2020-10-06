import datetime
import io
import pandas as pd
import os

def drop_create_table(conn, curs, table, current_dir):
  try:
    print(datetime.datetime.now(), 'DROP %s table!' % table)
    curs.execute('DROP TABLE %s;' % table)
    conn.commit()
  except Exception as e:
    print(e)
    conn.rollback()

    print(datetime.datetime.now(), 'CREATE %s table!' % table)
    curs.execute(open('%s\\sql\\%s.sql' % (os.getcwd(), table), 'r').read())
    conn.commit()
  else:
    print(datetime.datetime.now(), 'CREATE %s table!' % table)
    curs.execute(open('%s/sql/%s.sql' % (os.getcwd(), table), 'r').read())
    conn.commit()

def insert_array(conn, curs, table, current_rows, tmp, log=True, columns=None):
  if log:
    print(datetime.datetime.now(), 'Making DataFrame...')
  df = pd.DataFrame(tmp)

  if log:
    print(datetime.datetime.now(), 'Removing object from memory...')
  tmp = []

  output = io.StringIO()
  print('output : ', output)

  if log:
    print(datetime.datetime.now(), 'Converting DataFrame to CSV...')
  df.to_csv(output, header=False, index=False, sep='|', quotechar='\'')

  if log:
    print(datetime.datetime.now(), 'Removing DataFrame from memory...')
  del df

  output.getvalue()
  output.seek(0)

  if log:
    print(datetime.datetime.now(), 'Inserting data of %s table to database...' % table)
  try:
    if columns:
      print('curs :',curs)
      curs.copy_from(output, table, columns=(columns), sep='|', null='')
    else:
      curs.copy_from(output, table, sep='|', null='')
  except Exception as e:
    print(e)
    conn.rollback()
    conn.close()
    print(datetime.datetime.now(), 'Insert failed!', e)
  else:
    conn.commit()
    print(datetime.datetime.now(), 'Inserting %d rows completed!' % current_rows)

  output.close()
