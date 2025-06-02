from flask import Flask, render_template, redirect, url_for, request, send_file, flash
import os
import json
import csv
from io import StringIO, BytesIO
import sqlite3
import random
from datetime import datetime, timedelta
from calendar import monthrange

app = Flask(__name__)

def init_db():
    conn = sqlite3.connect(os.path.join('data', 'staff.db'))
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS staff (staff_id TEXT PRIMARY KEY, name TEXT, title TEXT, ward TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS shift (shift_id TEXT PRIMARY KEY, name TEXT, time TEXT, required_count INTEGER, ward TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS schedule (date TEXT, shift_id TEXT, staff_id TEXT)')
    conn.commit()
    conn.close()

init_db()

def get_db_connection():
    conn = sqlite3.connect(os.path.join('data', 'staff.db'))
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/staff')
def staff():
    conn = get_db_connection()
    staff_list = conn.execute('SELECT * FROM staff').fetchall()
    conn.close()
    return render_template('staff.html', staff_list=staff_list)

@app.route('/shift')
def shift():
    conn = get_db_connection()
    shift_list = conn.execute('SELECT * FROM shift').fetchall()
    conn.close()
    return render_template('shift.html', shift_list=shift_list)

@app.route('/schedule')
def schedule():
    # 預設本月 yyyy-mm
    today = datetime.today()
    default_month = today.strftime('%Y-%m')
    return render_template('schedule.html', default_month=default_month)

@app.route('/view_schedule', methods=['GET', 'POST'])
def view_schedule():
    filters = {
        'date': '',
        'shift_name': '',
        'ward': '',
        'staff_name': ''
    }
    query = '''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward, 
               COALESCE(staff.name, '缺人值班') as staff_name, staff.staff_id
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        LEFT JOIN staff ON schedule.staff_id = staff.staff_id
        WHERE 1=1
    '''
    params = []
    if request.method == 'POST':
        filters['date'] = request.form.get('date', '')
        filters['shift_name'] = request.form.get('shift_name', '')
        filters['ward'] = request.form.get('ward', '')
        filters['staff_name'] = request.form.get('staff_name', '')
        if filters['date']:
            query += ' AND schedule.date = ?'
            params.append(filters['date'])
        if filters['shift_name']:
            query += ' AND shift.name LIKE ?'
            params.append(f"%{filters['shift_name']}%")
        if filters['ward']:
            query += ' AND shift.ward LIKE ?'
            params.append(f"%{filters['ward']}%")
        if filters['staff_name']:
            query += ' AND staff.name LIKE ?'
            params.append(f"%{filters['staff_name']}%")
    query += ' ORDER BY schedule.date, shift.name'
    conn = get_db_connection()
    schedule = conn.execute(query, params).fetchall()
    # 計算本月班數統計
    staff_stats = []
    if schedule:
        # 取出本月
        first_date = schedule[0]['date']
        month = first_date[:7]  # yyyy-mm
        staff_count = {}
        staff_name_map = {}
        for row in schedule:
            if row['date'][:7] == month:
                sid = row['staff_id']
                if sid is None:
                    continue
                staff_count[sid] = staff_count.get(sid, 0) + 1
                staff_name_map[sid] = row['staff_name']
        for sid, count in staff_count.items():
            staff_stats.append({'staff_id': sid, 'name': staff_name_map[sid], 'count': count})
        staff_stats.sort(key=lambda x: x['staff_id'])
    conn.close()
    return render_template('view_schedule.html', schedule=schedule, filters=filters, staff_stats=staff_stats)

@app.route('/add_staff', methods=['POST'])
def add_staff():
    staff_id = request.form['staff_id']
    name = request.form['name']
    title = request.form['title']
    ward = request.form['ward']
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO staff (staff_id, name, title, ward) VALUES (?, ?, ?, ?)', (staff_id, name, title, ward))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # 可加上提示：員工編號重複
    conn.close()
    return redirect(url_for('staff'))

@app.route('/delete_staff', methods=['POST'])
def delete_staff():
    staff_id = request.form['staff_id']
    conn = get_db_connection()
    conn.execute('DELETE FROM staff WHERE staff_id = ?', (staff_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('staff'))

@app.route('/edit_staff', methods=['POST'])
def edit_staff():
    staff_id = request.form['staff_id']
    name = request.form['name']
    title = request.form['title']
    ward = request.form['ward']
    conn = get_db_connection()
    conn.execute('UPDATE staff SET name = ?, title = ?, ward = ? WHERE staff_id = ?', (name, title, ward, staff_id))
    conn.commit()
    conn.close()
    return redirect(url_for('staff'))

@app.route('/download_staff_template')
def download_staff_template():
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['staff_id', 'name', 'title', 'ward'])
    writer.writerow(['N001', '王小明', '護理師', 'A病房'])
    writer.writerow(['N002', '李小華', '護理長', 'B病房'])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='staff_template.csv'
    )

@app.route('/upload_staff', methods=['POST'])
def upload_staff():
    file = request.files.get('file')
    if not file:
        flash('請選擇檔案')
        return redirect(url_for('staff'))
    stream = StringIO(file.stream.read().decode('utf-8-sig'))
    reader = csv.DictReader(stream)
    conn = get_db_connection()
    for row in reader:
        if row.get('staff_id') and row.get('name') and row.get('title') and row.get('ward'):
            try:
                conn.execute('INSERT INTO staff (staff_id, name, title, ward) VALUES (?, ?, ?, ?)', (row['staff_id'], row['name'], row['title'], row['ward']))
            except sqlite3.IntegrityError:
                pass  # 跳過重複的員工編號
    conn.commit()
    conn.close()
    return redirect(url_for('staff'))

@app.route('/add_shift', methods=['POST'])
def add_shift():
    shift_id = request.form['shift_id']
    name = request.form['name']
    time = request.form['time']
    required_count = request.form['required_count']
    ward = request.form['ward']
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO shift (shift_id, name, time, required_count, ward) VALUES (?, ?, ?, ?, ?)', (shift_id, name, time, required_count, ward))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()
    return redirect(url_for('shift'))

@app.route('/download_shift_template')
def download_shift_template():
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['shift_id', 'name', 'time', 'required_count', 'ward'])
    writer.writerow(['S1', '早班', '08:00-16:00', '3', 'A病房'])
    writer.writerow(['S2', '晚班', '16:00-00:00', '2', 'B病房'])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='shift_template.csv'
    )

@app.route('/upload_shift', methods=['POST'])
def upload_shift():
    file = request.files.get('file')
    if not file:
        flash('請選擇檔案')
        return redirect(url_for('shift'))
    stream = StringIO(file.stream.read().decode('utf-8-sig'))
    reader = csv.DictReader(stream)
    conn = get_db_connection()
    for row in reader:
        if row.get('shift_id') and row.get('name') and row.get('time') and row.get('required_count') and row.get('ward'):
            try:
                conn.execute('INSERT INTO shift (shift_id, name, time, required_count, ward) VALUES (?, ?, ?, ?, ?)', (row['shift_id'], row['name'], row['time'], row['required_count'], row['ward']))
            except sqlite3.IntegrityError:
                pass  # 跳過重複的班別編號
    conn.commit()
    conn.close()
    return redirect(url_for('shift'))

@app.route('/edit_shift', methods=['POST'])
def edit_shift():
    shift_id = request.form['shift_id']
    name = request.form['name']
    time = request.form['time']
    required_count = request.form['required_count']
    ward = request.form['ward']
    conn = get_db_connection()
    conn.execute('UPDATE shift SET name = ?, time = ?, required_count = ?, ward = ? WHERE shift_id = ?', (name, time, required_count, ward, shift_id))
    conn.commit()
    conn.close()
    return redirect(url_for('shift'))

@app.route('/delete_shift', methods=['POST'])
def delete_shift():
    shift_id = request.form['shift_id']
    conn = get_db_connection()
    conn.execute('DELETE FROM shift WHERE shift_id = ?', (shift_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('shift'))

@app.route('/auto_schedule', methods=['POST'])
def auto_schedule():
    # 取得使用者選擇的月份與排班參數
    month = request.form.get('month')
    max_per_day = int(request.form.get('max_per_day', 1))
    max_consecutive = int(request.form.get('max_consecutive', 5))
    min_per_month = int(request.form.get('min_per_month', 22))
    max_per_month = int(request.form.get('max_per_month', 30))
    max_night_consecutive = int(request.form.get('max_night_consecutive', 2))
    max_night_per_month = int(request.form.get('max_night_per_month', 8))
    auto_fill_missing = request.form.get('auto_fill_missing', 'yes') == 'yes'
    fair_distribution = request.form.get('fair_distribution', 'yes') == 'yes'
    special_preference = request.form.get('special_preference', 'no') == 'yes'
    if not month:
        today = datetime.today()
        month = today.strftime('%Y-%m')
    year, mon = map(int, month.split('-'))
    days_in_month = monthrange(year, mon)[1]
    dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
    conn = get_db_connection()
    shifts = conn.execute('SELECT * FROM shift').fetchall()
    staff = conn.execute('SELECT * FROM staff').fetchall()
    staff_list = [dict(s) for s in staff]
    # 初始化每人本月已排班數與連續上班天數、夜班統計
    staff_status = {s['staff_id']: {'count': 0, 'consecutive': 0, 'last_date': None, 'last_worked': False, 'shift_counts': {}, 'night_count': 0, 'night_consecutive': 0, 'last_night_date': None, 'last_night_worked': False} for s in staff_list}
    conn.execute('DELETE FROM schedule')
    for date in dates:
        for shift in shifts:
            required = int(shift['required_count'])
            ward = shift['ward']
            is_night = '夜' in shift['name']
            available_staff = [s for s in staff_list if s['ward'] == ward]
            candidates = []
            for s in available_staff:
                st = staff_status[s['staff_id']]
                if st.get('today', '') == date:
                    continue
                # 每日最多班數
                if st['shift_counts'].get(date, 0) >= max_per_day:
                    continue
                # 連續上班天數
                if st['last_worked'] and st['last_date']:
                    last = datetime.strptime(st['last_date'], '%Y-%m-%d').date()
                    if (datetime.strptime(date, '%Y-%m-%d').date() - last).days == 1:
                        if st['consecutive'] >= max_consecutive:
                            continue
                # 每月班數上限
                if st['count'] >= max_per_month:
                    continue
                # 夜班限制
                if is_night:
                    if st['night_count'] >= max_night_per_month:
                        continue
                    if st['last_night_worked'] and st['last_night_date']:
                        last_night = datetime.strptime(st['last_night_date'], '%Y-%m-%d').date()
                        if (datetime.strptime(date, '%Y-%m-%d').date() - last_night).days == 1:
                            if st['night_consecutive'] >= max_night_consecutive:
                                continue
                # --- 新增班別間最小間隔11小時 ---
                # 需有上一次排班資訊
                if st['last_date'] and st['last_worked']:
                    last_date = st['last_date']
                    # 找出上一次班別的下班時間
                    last_shift_id = None
                    for sh in shifts:
                        if sh['shift_id'] in st['shift_counts'] and sh['shift_id'] != shift['shift_id']:
                            if sh['ward'] == ward:
                                last_shift_id = sh['shift_id']
                    if last_shift_id:
                        # 取得上一次班別的時間
                        last_shift = next((sh for sh in shifts if sh['shift_id'] == last_shift_id), None)
                        if last_shift:
                            try:
                                last_end = last_shift['time'].split('-')[1]
                                last_end_dt = datetime.strptime(f"{last_date} {last_end}", "%Y-%m-%d %H:%M")
                                this_start = shift['time'].split('-')[0]
                                this_start_dt = datetime.strptime(f"{date} {this_start}", "%Y-%m-%d %H:%M")
                                # 若間隔小於11小時則跳過
                                if (this_start_dt - last_end_dt).total_seconds() < 11*3600:
                                    continue
                            except Exception:
                                pass
                shift_count = st['shift_counts'].get(shift['shift_id'], 0)
                candidates.append((s, st['count'], shift_count))
            # 公平分配：優先排班數少、該班別少的人
            if fair_distribution:
                candidates.sort(key=lambda x: (x[1], x[2]))
            else:
                random.shuffle(candidates)
            assigned = []
            for c in candidates:
                if len(assigned) >= required:
                    break
                s = c[0]
                st = staff_status[s['staff_id']]
                if st.get('today', '') == date:
                    continue
                assigned.append(s)
                st['count'] += 1
                st['shift_counts'][shift['shift_id']] = st['shift_counts'].get(shift['shift_id'], 0) + 1
                st['shift_counts'][date] = st['shift_counts'].get(date, 0) + 1
                if st['last_worked'] and st['last_date']:
                    last = datetime.strptime(st['last_date'], '%Y-%m-%d').date()
                    if (datetime.strptime(date, '%Y-%m-%d').date() - last).days == 1:
                        st['consecutive'] += 1
                    else:
                        st['consecutive'] = 1
                else:
                    st['consecutive'] = 1
                st['last_date'] = date
                st['last_worked'] = True
                st['today'] = date
                # 夜班統計
                if is_night:
                    st['night_count'] += 1
                    if st['last_night_worked'] and st['last_night_date']:
                        last_night = datetime.strptime(st['last_night_date'], '%Y-%m-%d').date()
                        if (datetime.strptime(date, '%Y-%m-%d').date() - last_night).days == 1:
                            st['night_consecutive'] += 1
                        else:
                            st['night_consecutive'] = 1
                    else:
                        st['night_consecutive'] = 1
                    st['last_night_date'] = date
                    st['last_night_worked'] = True
                else:
                    st['night_consecutive'] = 0
                    st['last_night_worked'] = False
            for s in assigned:
                conn.execute('INSERT INTO schedule (date, shift_id, staff_id) VALUES (?, ?, ?)', (date, shift['shift_id'], s['staff_id']))
            if auto_fill_missing:
                for _ in range(required - len(assigned)):
                    conn.execute('INSERT INTO schedule (date, shift_id, staff_id) VALUES (?, ?, ?)', (date, shift['shift_id'], '缺人值班'))
        for st in staff_status.values():
            st['today'] = ''
    conn.commit()
    conn.close()
    return redirect(url_for('calendar_view', month=month))

@app.route('/export_schedule', methods=['POST'])
def export_schedule():
    filters = {
        'date': request.form.get('date', ''),
        'shift_name': request.form.get('shift_name', ''),
        'ward': request.form.get('ward', ''),
        'staff_name': request.form.get('staff_name', '')
    }
    query = '''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward, staff.name as staff_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        JOIN staff ON schedule.staff_id = staff.staff_id
        WHERE 1=1
    '''
    params = []
    if filters['date']:
        query += ' AND schedule.date = ?'
        params.append(filters['date'])
    if filters['shift_name']:
        query += ' AND shift.name LIKE ?'
        params.append(f"%{filters['shift_name']}%")
    if filters['ward']:
        query += ' AND shift.ward LIKE ?'
        params.append(f"%{filters['ward']}%")
    if filters['staff_name']:
        query += ' AND staff.name LIKE ?'
        params.append(f"%{filters['staff_name']}%")
    query += ' ORDER BY schedule.date, shift.name'
    conn = get_db_connection()
    schedule = conn.execute(query, params).fetchall()
    conn.close()
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['日期', '班別', '病房', '人員'])
    for row in schedule:
        writer.writerow([row['date'], row['shift_name'], row['ward'], row['staff_name']])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='schedule_export.csv'
    )

@app.route('/pivot_schedule')
def pivot_schedule():
    conn = get_db_connection()
    schedule = conn.execute('''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward, staff.name as staff_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        JOIN staff ON schedule.staff_id = staff.staff_id
    ''').fetchall()
    conn.close()
    # 轉成list of dicts
    data = [dict(row) for row in schedule]
    return render_template('pivot_schedule.html', data=data)

@app.route('/calendar_view')
def calendar_view():
    # 取得本月
    today = datetime.today()
    month = request.args.get('month', today.strftime('%Y-%m'))
    year, mon = map(int, month.split('-'))
    days_in_month = monthrange(year, mon)[1]
    conn = get_db_connection()
    schedule = conn.execute('''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward,
               COALESCE(staff.name, '缺人值班') as staff_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        LEFT JOIN staff ON schedule.staff_id = staff.staff_id
        WHERE schedule.date BETWEEN ? AND ?
        ORDER BY schedule.date, shift.name
    ''', (f"{year}-{mon:02d}-01", f"{year}-{mon:02d}-{days_in_month:02d}")).fetchall()
    conn.close()
    # 轉換為 FullCalendar events 格式
    events = []
    for row in schedule:
        event = {
            'title': f"{row['shift_name']} - {row['staff_name']}",
            'start': row['date'],
            'description': f"病房: {row['ward']}"
        }
        if row['staff_name'] == '缺人值班':
            event['className'] = ['fc-missing']
        events.append(event)
    return render_template('calendar_view.html', month=month, events=events)

if __name__ == '__main__':
    app.run(debug=True) 