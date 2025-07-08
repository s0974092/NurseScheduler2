from flask import Flask, render_template, redirect, url_for, request, send_file, flash, session, jsonify
import os
import json
import csv
from io import StringIO, BytesIO
import sqlite3
import random
from datetime import datetime, timedelta
from calendar import monthrange
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps

app = Flask(__name__)
app.secret_key = 'nurse-secret-key'  # session 用

def init_db():
    conn = sqlite3.connect(os.path.join('data', 'staff.db'))
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS staff (staff_id TEXT PRIMARY KEY, name TEXT, title TEXT, ward TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS shift (shift_id TEXT PRIMARY KEY, name TEXT, time TEXT, required_count INTEGER, ward TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS schedule (date TEXT, shift_id TEXT, staff_id TEXT, work_hours INTEGER, is_auto INTEGER, operator_id TEXT, created_at TEXT, updated_at TEXT)')
    
    # 新增：人員排班偏好設定
    c.execute('''CREATE TABLE IF NOT EXISTS staff_preference (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        staff_id TEXT NOT NULL,
        month TEXT NOT NULL,  -- yyyy-mm 格式
        preference_type TEXT NOT NULL,  -- 'single' 或 'dual'
        shift_id_1 TEXT,  -- 主要班別
        shift_id_2 TEXT,  -- 次要班別（雙班別時使用）
        week_pattern TEXT,  -- 'alternate' 或 'consecutive'（雙班別時的週次模式）
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
        FOREIGN KEY (shift_id_1) REFERENCES shift (shift_id),
        FOREIGN KEY (shift_id_2) REFERENCES shift (shift_id),
        UNIQUE(staff_id, month)
    )''')
    
    # 新增：假日 on call 排班
    c.execute('''CREATE TABLE IF NOT EXISTS oncall_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        staff_id TEXT NOT NULL,
        status TEXT DEFAULT 'oncall',  -- 'oncall', 'backup', 'off'
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
        UNIQUE(date, staff_id)
    )''')
    
    # 權限管理：新增 user 資料表
    c.execute('''CREATE TABLE IF NOT EXISTS user (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        staff_id TEXT
    )''')
    
    # 新增：班表異動紀錄
    c.execute('''CREATE TABLE IF NOT EXISTS schedule_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        schedule_id INTEGER,
        action TEXT NOT NULL,  -- 'create', 'update', 'delete'
        old_data TEXT,  -- JSON 格式的舊資料
        new_data TEXT,  -- JSON 格式的新資料
        operator_id TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 新增：四周變形工時設定表
    c.execute('''CREATE TABLE IF NOT EXISTS work_schedule_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT NOT NULL,  -- yyyy-mm 格式
        is_flexible_workweek BOOLEAN DEFAULT 1,  -- 預設啟用四周變形工時
        require_holiday BOOLEAN DEFAULT 1,  -- 每人每週是否需要例假日
        require_rest_day BOOLEAN DEFAULT 1,  -- 每人每週是否需要休息日
        holiday_day INTEGER DEFAULT 7,  -- 例假日：1=週一, 2=週二, ..., 7=週日
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(month)
    )''')
    
    # 新增：班別每日需求人數表
    c.execute('''CREATE TABLE IF NOT EXISTS shift_daily_requirements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_id TEXT NOT NULL,
        day_of_week INTEGER NOT NULL,  -- 1=週一, 2=週二, ..., 7=週日
        required_count INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (shift_id) REFERENCES shift (shift_id),
        UNIQUE(shift_id, day_of_week)
    )''')
    
    # 新增：個人週工時統計表
    c.execute('''CREATE TABLE IF NOT EXISTS weekly_work_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        staff_id TEXT NOT NULL,
        month TEXT NOT NULL,  -- yyyy-mm 格式
        week_number INTEGER NOT NULL,  -- 1-4 週
        total_hours INTEGER DEFAULT 0,  -- 該週總工時
        holiday_count INTEGER DEFAULT 0,  -- 例假日天數
        rest_day_count INTEGER DEFAULT 0,  -- 休息日天數
        work_days INTEGER DEFAULT 0,  -- 工作天數
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
        UNIQUE(staff_id, month, week_number)
    )''')
    
    # 檢查是否已有 admin 帳號，若無則建立預設管理員
    admin = c.execute('SELECT * FROM user WHERE username = ?', ('admin',)).fetchone()
    if not admin:
        pw_hash = generate_password_hash('admin123')
        c.execute('INSERT INTO user (username, password_hash, role) VALUES (?, ?, ?)', ('admin', pw_hash, 'admin'))
    conn.commit()
    conn.close()

init_db()

def get_db_connection():
    conn = sqlite3.connect(os.path.join('data', 'staff.db'))
    conn.row_factory = sqlite3.Row
    return conn

def migrate_existing_data():
    """將現有資料遷移到新結構"""
    conn = get_db_connection()
    
    try:
        # 1. 將現有班別的 required_count 複製到每日需求表
        shifts = conn.execute('SELECT shift_id, required_count FROM shift').fetchall()
        for shift in shifts:
            for day in range(1, 8):  # 週一到週日
                conn.execute('''INSERT OR IGNORE INTO shift_daily_requirements 
                               (shift_id, day_of_week, required_count) 
                               VALUES (?, ?, ?)''', 
                           (shift['shift_id'], day, shift['required_count']))
        
        # 2. 為現有月份建立預設的四周變形工時設定
        months = conn.execute('''
            SELECT DISTINCT substr(date, 1, 7) as month 
            FROM schedule 
            ORDER BY month
        ''').fetchall()
        
        for month_row in months:
            month = month_row['month']
            conn.execute('''INSERT OR IGNORE INTO work_schedule_config 
                           (month, is_flexible_workweek, require_holiday, require_rest_day, holiday_day) 
                           VALUES (?, 1, 1, 1, 7)''', (month,))
        
        conn.commit()
        print("資料遷移完成")
    except Exception as e:
        print(f"資料遷移失敗：{str(e)}")
    finally:
        conn.close()

# 執行資料遷移
migrate_existing_data()

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'role' not in session or session['role'] != 'admin':
            flash('權限不足，僅限管理員操作', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/staff')
@login_required
def staff():
    conn = get_db_connection()
    staff_list = conn.execute('SELECT * FROM staff').fetchall()
    conn.close()
    return render_template('staff.html', staff_list=staff_list)

@app.route('/shift')
@login_required
def shift():
    conn = get_db_connection()
    shift_list = conn.execute('SELECT * FROM shift').fetchall()
    
    # 取得每個班別的每日需求人數
    for shift in shift_list:
        daily_reqs = conn.execute('SELECT day_of_week, required_count FROM shift_daily_requirements WHERE shift_id = ? ORDER BY day_of_week', 
                                 (shift['shift_id'],)).fetchall()
        
        # 初始化每日需求人數
        shift_dict = dict(shift)
        for day in range(1, 8):
            shift_dict[f'day_{day}_count'] = shift['required_count']  # 預設值
        
        # 填入實際的每日需求人數
        for req in daily_reqs:
            day_num = req['day_of_week']
            shift_dict[f'day_{day_num}_count'] = req['required_count']
        
        # 為了模板相容性，也設定週一到週日的別名
        shift_dict['monday_count'] = shift_dict.get('day_1_count', shift['required_count'])
        shift_dict['tuesday_count'] = shift_dict.get('day_2_count', shift['required_count'])
        shift_dict['wednesday_count'] = shift_dict.get('day_3_count', shift['required_count'])
        shift_dict['thursday_count'] = shift_dict.get('day_4_count', shift['required_count'])
        shift_dict['friday_count'] = shift_dict.get('day_5_count', shift['required_count'])
        shift_dict['saturday_count'] = shift_dict.get('day_6_count', shift['required_count'])
        shift_dict['sunday_count'] = shift_dict.get('day_7_count', shift['required_count'])
        
        # 更新shift_list中的資料
        shift_list = [shift_dict if s['shift_id'] == shift['shift_id'] else s for s in shift_list]
    
    conn.close()
    return render_template('shift.html', shift_list=shift_list)

@app.route('/schedule')
@login_required
def schedule():
    # 預設本月 yyyy-mm
    today = datetime.today()
    default_month = today.strftime('%Y-%m')
    return render_template('schedule.html', default_month=default_month)

@app.route('/view_schedule', methods=['GET', 'POST'])
@login_required
def view_schedule():
    filters = {
        'date': '',
        'shift_name': '',
        'ward': '',
        'staff_name': ''
    }
    query = '''
        SELECT schedule.id, schedule.date, shift.name as shift_name, shift.ward as ward, 
               COALESCE(staff.name, '缺人值班') as staff_name, staff.staff_id,
               schedule.work_hours, schedule.status, schedule.remark,
               oncall_schedule.status as oncall_status
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        LEFT JOIN staff ON schedule.staff_id = staff.staff_id
        LEFT JOIN oncall_schedule ON schedule.date = oncall_schedule.date AND schedule.staff_id = oncall_schedule.staff_id
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
    # 將每筆資料加上工時欄位
    schedule = [dict(row) for row in schedule]
    for row in schedule:
        row['work_hours'] = row.get('work_hours', 8)
    # 取得所有員工清單（供下拉選單用）
    staff_rows = conn.execute('SELECT staff_id, name FROM staff').fetchall()
    staff_list = [dict(row) for row in staff_rows]
    conn.close()
    return render_template('view_schedule.html', schedule=schedule, filters=filters, staff_stats=staff_stats, staff_list=staff_list)

@app.route('/add_staff', methods=['POST'])
@login_required
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
@login_required
def delete_staff():
    staff_id = request.form['staff_id']
    conn = get_db_connection()
    conn.execute('DELETE FROM staff WHERE staff_id = ?', (staff_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('staff'))

@app.route('/edit_staff', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
def add_shift():
    shift_id = request.form['shift_id']
    name = request.form['name']
    time = request.form['time']
    required_count = request.form['required_count']
    ward = request.form['ward']
    
    # 取得每日需求人數
    daily_requirements = {
        'monday_count': request.form.get('monday_count', required_count),
        'tuesday_count': request.form.get('tuesday_count', required_count),
        'wednesday_count': request.form.get('wednesday_count', required_count),
        'thursday_count': request.form.get('thursday_count', required_count),
        'friday_count': request.form.get('friday_count', required_count),
        'saturday_count': request.form.get('saturday_count', required_count),
        'sunday_count': request.form.get('sunday_count', required_count)
    }
    
    conn = get_db_connection()
    try:
        # 新增班別
        conn.execute('INSERT INTO shift (shift_id, name, time, required_count, ward) VALUES (?, ?, ?, ?, ?)', 
                    (shift_id, name, time, required_count, ward))
        
        # 新增每日需求人數設定
        for day_of_week, count in enumerate(daily_requirements.values(), 1):
            conn.execute('INSERT INTO shift_daily_requirements (shift_id, day_of_week, required_count) VALUES (?, ?, ?)',
                        (shift_id, day_of_week, count))
        
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()
    return redirect(url_for('shift'))

@app.route('/download_shift_template')
@login_required
def download_shift_template():
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['shift_id', 'name', 'time', 'required_count', 'ward'])
    writer.writerow(['S1', '早班', '07:00-15:00', '3', 'A病房'])
    writer.writerow(['S2', '小夜班', '15:00-23:00', '2', 'B病房'])
    writer.writerow(['S3', '大夜班', '23:00-07:00', '1', 'C病房'])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='shift_template.csv'
    )

@app.route('/upload_shift', methods=['POST'])
@login_required
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
@login_required
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
@login_required
def delete_shift():
    shift_id = request.form['shift_id']
    conn = get_db_connection()
    conn.execute('DELETE FROM shift WHERE shift_id = ?', (shift_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('shift'))

@app.route('/save_daily_requirements', methods=['POST'])
@login_required
def save_daily_requirements():
    try:
        data = request.get_json()
        shift_id = data.get('shift_id')
        requirements = data.get('requirements', {})
        
        conn = get_db_connection()
        
        # 更新每日需求人數
        for day_key, count in requirements.items():
            if day_key.startswith('day_'):
                day_of_week = int(day_key.split('_')[1])
                count = int(count) if count else 0
                
                # 使用 INSERT OR REPLACE 來更新資料
                conn.execute('''INSERT OR REPLACE INTO shift_daily_requirements 
                               (shift_id, day_of_week, required_count) 
                               VALUES (?, ?, ?)''', 
                           (shift_id, day_of_week, count))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': '每日需求人數儲存成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/auto_schedule', methods=['POST'])
@login_required
def auto_schedule():
    # 取出前端傳過來的月份，並標準化成 YYYY-MM
    raw_month = request.form.get('month')
    year, mon = map(int, raw_month.split('-'))
    month = f"{year:04d}-{mon:02d}"
    max_per_day = int(request.form.get('max_per_day', 1))
    max_consecutive = int(request.form.get('max_consecutive', 5))
    min_per_month = int(request.form.get('min_per_month', 22))
    max_per_month = int(request.form.get('max_per_month', 30))
    max_night_consecutive = int(request.form.get('max_night_consecutive', 2))
    max_night_per_month = int(request.form.get('max_night_per_month', 8))
    auto_fill_missing = request.form.get('auto_fill_missing', 'yes') == 'yes'
    fair_distribution = request.form.get('fair_distribution', 'yes') == 'yes'
    special_preference = request.form.get('special_preference', 'no') == 'yes'
    
    # 四周變形工時參數
    is_flexible_workweek = request.form.get('is_flexible_workweek', 'yes') == 'yes'
    require_holiday = request.form.get('require_holiday', 'yes') == 'yes'
    require_rest_day = request.form.get('require_rest_day', 'yes') == 'yes'
    holiday_day = int(request.form.get('holiday_day', 7))  # 預設週日
    
    # 如果前端沒傳，或格式不對，就退回今天
    if not raw_month:
        today = datetime.today()
        month = today.strftime('%Y-%m')
    year, mon = map(int, month.split('-'))
    days_in_month = monthrange(year, mon)[1]
    dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
    
    conn = get_db_connection()
    conn.execute('''INSERT OR REPLACE INTO work_schedule_config 
                    (month, is_flexible_workweek, require_holiday, require_rest_day, holiday_day) 
                    VALUES (?, ?, ?, ?, ?)''', 
                (month, is_flexible_workweek, require_holiday, require_rest_day, holiday_day))
    
    shifts = conn.execute('SELECT * FROM shift').fetchall()
    staff = conn.execute('SELECT * FROM staff').fetchall()
    staff_list = [dict(s) for s in staff]
    
    # 每日需求人數
    daily_requirements = {}
    for shift in shifts:
        daily_requirements[shift['shift_id']] = {}
        for day in range(1, 8):
            req = conn.execute(
                'SELECT required_count FROM shift_daily_requirements WHERE shift_id = ? AND day_of_week = ?', 
                (shift['shift_id'], day)
            ).fetchone()
            daily_requirements[shift['shift_id']][day] = req['required_count'] if req else shift['required_count']
    
    # 特殊偏好
    preferences = {}
    if special_preference:
        pref_rows = conn.execute(
            'SELECT * FROM staff_preference WHERE month = ?', (month,)
        ).fetchall()
        for pref in pref_rows:
            preferences[pref['staff_id']] = {
                'type': pref['preference_type'],
                'shift_id_1': pref['shift_id_1'],
                'shift_id_2': pref['shift_id_2'],
                'week_pattern': pref['week_pattern']
            }
    
    # 初始化狀態
    staff_status = {
        s['staff_id']: {
            'count': 0,
            'consecutive': 0,
            'last_date': None,
            'last_worked': False,
            'shift_counts': {},
            'night_count': 0,
            'night_consecutive': 0,
            'last_night_date': None,
            'last_night_worked': False,
            'week1_count': 0, 'week2_count': 0, 'week3_count': 0,
            'week4_count': 0, 'week5_count': 0, 'week6_count': 0,
            'weekly_hours': {i: 0 for i in range(1,7)},
            'holiday_days': {i: 0 for i in range(1,7)},
            'rest_days': {i: 0 for i in range(1,7)},
            'worked_days': {i: 0 for i in range(1,7)},
        } for s in staff_list
    }
    
    # 清掉舊排班
    conn.execute('DELETE FROM schedule WHERE date LIKE ?', (f"{month}%",))
    conn.execute('DELETE FROM weekly_work_stats WHERE month = ?', (month,))
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    operator = session.get('username', 'system')
    
    # 先分配每週例假(週日)與休息日(週六或其他)
    staff_holidays = {s['staff_id']: {} for s in staff_list}
    staff_restdays = {s['staff_id']: {} for s in staff_list}
    for s in staff_list:
        sid = s['staff_id']
        for w in range(1,7):
            sundays = [d for d in dates 
                       if (datetime.strptime(d, '%Y-%m-%d').weekday()+1)==7
                       and ((int(d[-2:])-1)//7+1)==w]
            if sundays:
                staff_holidays[sid][w] = sundays[0]

    # 每人每週從週一～週六隨機分配一天休息
    staff_restdays = {s['staff_id']: {} for s in staff_list}
    for s in staff_list:
        sid = s['staff_id']
        for w in range(1, 7):
            # 該週所有週一～週六的日期
            week_days = [
                d for d in dates
                if ((int(d[-2:]) - 1) // 7 + 1) == w
                   and (datetime.strptime(d, '%Y-%m-%d').weekday() + 1) in range(1, 7)
            ]
            # 隨機挑一日當休息日
            if week_days:
                staff_restdays[sid][w] = random.choice(week_days)
    
    # 主迴圈：每日排班
    for date in dates:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        week_of_month = (date_obj.day-1)//7 + 1
        day_of_week = date_obj.weekday() + 1
        is_holiday = (day_of_week == holiday_day)
        date_worked = set()
        
        for shift in shifts:
            required = daily_requirements[shift['shift_id']][day_of_week]
            ward = shift['ward']
            is_night = '大夜' in shift['name']
            available_staff = [s for s in staff_list if s['ward'] == ward]
            candidates = []
            
            for s in available_staff:
                sid = s['staff_id']
                st = staff_status[sid]
                # 只有在「非夜班」時才跳過例假／休息日
                if not is_night and (
                   staff_holidays[sid].get(week_of_month) == date or
                   staff_restdays[sid].get(week_of_month) == date):
                    continue
                if st.get('today') == date:
                    continue
                if st['shift_counts'].get(date,0) >= max_per_day or st['count'] >= max_per_month:
                    continue
                if is_flexible_workweek:
                    w = min(week_of_month,6)
                    if st['weekly_hours'][w] >= 40:
                        continue
                    if require_holiday and is_holiday and st['holiday_days'][w] > 0:
                        continue

                # 連續上班檢查略…
                # 夜班限制略…
                # 11 小時間隔檢查略…
                candidates.append((s, st['count'], st['shift_counts'].get(shift['shift_id'],0)))
            
            if fair_distribution:
                candidates.sort(key=lambda x: (x[1], x[2]))
            else:
                random.shuffle(candidates)
            
            assigned = []
            for c in candidates[:required]:
                s = c[0]
                sid = s['staff_id']
                st = staff_status[sid]
                if st.get('today') == date:
                    continue
                assigned.append(s)
                
                # 更新狀態
                st['count'] += 1
                st['shift_counts'][shift['shift_id']] = st['shift_counts'].get(shift['shift_id'],0) + 1
                st['shift_counts'][date] = st['shift_counts'].get(date,0) + 1
                st['last_date']    = date
                st['last_worked']  = True
                st['today']        = date
                
                # 週次、工時、節假日統計
                w = min(week_of_month,6)
                st[f'week{w}_count'] += 1
                st['weekly_hours'][w]  += 8
                if is_holiday:
                    st['holiday_days'][w] += 1
                st['worked_days'][w]   += 1
                
                # 夜班統計略…
                date_worked.add(sid)
            
            # 插入排班
            for s in assigned:
                conn.execute(
                    'INSERT INTO schedule (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (date, shift['shift_id'], s['staff_id'], 8, 1, operator, now_str, now_str)
                )
            if auto_fill_missing:
                for _ in range(required - len(assigned)):
                    conn.execute(
                        'INSERT INTO schedule (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        (date, shift['shift_id'], '缺人值班', 8, 1, operator, now_str, now_str)
                    )
        
        # 當日未上班者累 rest_days
        if not is_holiday:
            for s in staff_list:
                sid = s['staff_id']
                if sid not in date_worked:
                    w = min(week_of_month,6)
                    staff_status[sid]['rest_days'][w] += 1
                
        
        # 重置當日標記
        for st in staff_status.values():
            st['today'] = ''
    
    # 儲存週工時統計
    for staff_id, st in staff_status.items():
        for w in range(1,7):
            conn.execute(
                'INSERT INTO weekly_work_stats (staff_id, month, week_number, total_hours, holiday_count, rest_day_count, work_days) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (staff_id, month, w,
                 st['weekly_hours'][w], st['holiday_days'][w],
                 st['rest_days'][w],    st[f'week{w}_count'])
            )
    
    conn.commit()
    conn.close()
    return redirect(url_for('calendar_view', month=month))

@app.route('/export_schedule', methods=['POST'])
@login_required
def export_schedule():
    filters = {
        'date': request.form.get('date', ''),
        'shift_name': request.form.get('shift_name', ''),
        'ward': request.form.get('ward', ''),
        'staff_name': request.form.get('staff_name', '')
    }
    query = '''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward, staff.name as staff_name, schedule.work_hours
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
    writer.writerow(['日期', '班別', '病房', '人員', '工時'])
    for row in schedule:
        writer.writerow([row['date'], row['shift_name'], row['ward'], row['staff_name'], row['work_hours']])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='schedule_export.csv'
    )

@app.route('/pivot_schedule')
@login_required
def pivot_schedule():
    conn = get_db_connection()
    schedule = conn.execute('''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward, staff.name as staff_name, schedule.work_hours
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        JOIN staff ON schedule.staff_id = staff.staff_id
    ''').fetchall()
    conn.close()
    data = [dict(row) for row in schedule]
    return render_template('pivot_schedule.html', data=data)

@app.route('/calendar_view')
@login_required
def calendar_view():
    # 取得本月
    today = datetime.today()
    month = request.args.get('month', today.strftime('%Y-%m'))
    year, mon = map(int, month.split('-'))
    days_in_month = monthrange(year, mon)[1]
    # 新增：取得查詢參數
    name = request.args.get('name', '').strip()
    ward = request.args.get('ward', '').strip()
    shift_name = request.args.get('shift_name', '').strip()
    query = '''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward,
               COALESCE(staff.name, '缺人值班') as staff_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        LEFT JOIN staff ON schedule.staff_id = staff.staff_id
        WHERE schedule.date BETWEEN ? AND ?
    '''
    params = [f"{year}-{mon:02d}-01", f"{year}-{mon:02d}-{days_in_month:02d}"]
    if name:
        query += ' AND staff.name LIKE ?'
        params.append(f"%{name}%")
    if ward:
        query += ' AND shift.ward LIKE ?'
        params.append(f"%{ward}%")
    if shift_name:
        query += ' AND shift.name LIKE ?'
        params.append(f"%{shift_name}%")
    query += ' ORDER BY schedule.date, shift.name'
    conn = get_db_connection()
    schedule = conn.execute(query, params).fetchall()
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
    # 保留查詢參數給模板
    return render_template('calendar_view.html', month=month, events=events, name=name, ward=ward, shift_name=shift_name)

@app.route('/staff_schedule_table')
@login_required
def staff_schedule_table():
    today = datetime.today()
    month = request.args.get('month', today.strftime('%Y-%m'))
    year, mon = map(int, month.split('-'))
    days_in_month = monthrange(year, mon)[1]
    dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
    # 新增：計算每天的星期
    weekday_map = ['一', '二', '三', '四', '五', '六', '日']
    weekdays = [weekday_map[datetime.strptime(d, "%Y-%m-%d").weekday()] for d in dates]
    conn = get_db_connection()
    staff_list = conn.execute('SELECT staff_id, name, title FROM staff').fetchall()
    schedule = conn.execute('''
        SELECT schedule.date, schedule.staff_id, shift.name as shift_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        WHERE schedule.date BETWEEN ? AND ?
    ''', (dates[0], dates[-1])).fetchall()
    conn.close()
    schedule_map = {}
    for row in schedule:
        schedule_map.setdefault(row['staff_id'], {})[row['date']] = row['shift_name']
    table = []
    for staff in staff_list:
        row = {
            'name': staff['name'],
            'title': staff['title'],
            'total_hours': 0,
            'leave_hours': 0,
            'shifts': []
        }
        for d in dates:
            shift = schedule_map.get(staff['staff_id'], {}).get(d, '')
            row['shifts'].append(shift)
            if shift:
                row['total_hours'] += 8
        table.append(row)
    # 傳遞 weekdays
    return render_template('staff_schedule_table.html', dates=dates, weekdays=weekdays, table=table)

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        conn = get_db_connection()
        user = conn.execute('SELECT * FROM user WHERE username = ?', (username,)).fetchone()
        conn.close()
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['user_id']
            session['username'] = user['username']
            session['role'] = user['role']
            flash('登入成功', 'success')
            return redirect(url_for('index'))
        else:
            error = '帳號或密碼錯誤'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    flash('已登出', 'info')
    return redirect(url_for('login'))

@app.route('/user_manage')
@login_required
@admin_required
def user_manage():
    conn = get_db_connection()
    users = conn.execute('SELECT * FROM user').fetchall()
    staff_list = conn.execute('SELECT staff_id, name FROM staff').fetchall()
    conn.close()
    return render_template('user_manage.html', users=users, staff_list=staff_list)

@app.route('/add_user', methods=['POST'])
@login_required
@admin_required
def add_user():
    username = request.form['username']
    password = request.form['password']
    role = request.form['role']
    staff_id = request.form.get('staff_id') or None
    pw_hash = generate_password_hash(password)
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO user (username, password_hash, role, staff_id) VALUES (?, ?, ?, ?)',
                     (username, pw_hash, role, staff_id))
        conn.commit()
        flash('新增使用者成功', 'success')
    except sqlite3.IntegrityError:
        flash('帳號重複，請更換', 'danger')
    conn.close()
    return redirect(url_for('user_manage'))

@app.route('/edit_user', methods=['POST'])
@login_required
@admin_required
def edit_user():
    user_id = request.form['user_id']
    password = request.form.get('password')
    role = request.form['role']
    staff_id = request.form.get('staff_id') or None
    conn = get_db_connection()
    if password:
        pw_hash = generate_password_hash(password)
        conn.execute('UPDATE user SET password_hash=?, role=?, staff_id=? WHERE user_id=?',
                     (pw_hash, role, staff_id, user_id))
    else:
        conn.execute('UPDATE user SET role=?, staff_id=? WHERE user_id=?',
                     (role, staff_id, user_id))
    conn.commit()
    conn.close()
    flash('修改使用者成功', 'success')
    return redirect(url_for('user_manage'))

@app.route('/delete_user', methods=['POST'])
@login_required
@admin_required
def delete_user():
    user_id = request.form['user_id']
    conn = get_db_connection()
    conn.execute('DELETE FROM user WHERE user_id=?', (user_id,))
    conn.commit()
    conn.close()
    flash('刪除使用者成功', 'info')
    return redirect(url_for('user_manage'))

@app.route('/download_user_template')
@login_required
@admin_required
def download_user_template():
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['username', 'password', 'role', 'staff_id'])
    writer.writerow(['nurse01', '123456', 'staff', 'N001'])
    writer.writerow(['admin02', 'adminpw', 'admin', ''])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='user_template.csv'
    )

@app.route('/upload_user', methods=['POST'])
@login_required
@admin_required
def upload_user():
    file = request.files.get('file')
    if not file:
        flash('請選擇檔案', 'danger')
        return redirect(url_for('user_manage'))
    stream = StringIO(file.stream.read().decode('utf-8-sig'))
    reader = csv.DictReader(stream)
    conn = get_db_connection()
    count = 0
    for row in reader:
        if row.get('username') and row.get('password') and row.get('role'):
            pw_hash = generate_password_hash(row['password'])
            try:
                conn.execute('INSERT INTO user (username, password_hash, role, staff_id) VALUES (?, ?, ?, ?)',
                             (row['username'], pw_hash, row['role'], row.get('staff_id') or None))
                count += 1
            except sqlite3.IntegrityError:
                continue  # 跳過重複帳號
    conn.commit()
    conn.close()
    flash(f'成功匯入 {count} 筆使用者', 'success')
    return redirect(url_for('user_manage'))

@app.route('/edit_schedule', methods=['POST'])
@login_required
def edit_schedule():
    """
    編輯單一班表（schedule）資料，需提供原主鍵（id）與新資料。
    僅限管理員。
    """
    if session.get('role') != 'admin':
        flash('權限不足，僅限管理員操作', 'danger')
        return redirect(url_for('view_schedule'))
    schedule_id = request.form['id']
    new_staff_id = request.form['staff_id']
    new_work_hours = request.form.get('work_hours', 8)
    new_status = request.form.get('status', '')
    new_remark = request.form.get('remark', '')
    conn = get_db_connection()
    before = conn.execute('SELECT * FROM schedule WHERE id = ?', (schedule_id,)).fetchone()
    if not before:
        flash('找不到班表資料', 'danger')
        conn.close()
        return redirect(url_for('view_schedule'))
    before_data = dict(before)
    conn.execute('UPDATE schedule SET staff_id=?, work_hours=?, status=?, remark=?, updated_at=?, operator_id=? WHERE id=?',
                 (new_staff_id, new_work_hours, new_status, new_remark, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session.get('username', 'system'), schedule_id))
    after = conn.execute('SELECT * FROM schedule WHERE id = ?', (schedule_id,)).fetchone()
    after_data = dict(after)
    conn.execute('INSERT INTO schedule_log (schedule_id, action, before_data, after_data, operator_id, operated_at) VALUES (?, ?, ?, ?, ?, ?)',
                 (schedule_id, 'edit', json.dumps(before_data, ensure_ascii=False), json.dumps(after_data, ensure_ascii=False), session.get('username', 'system'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    flash('班表已更新', 'success')
    return redirect(url_for('view_schedule'))

@app.route('/delete_schedule', methods=['POST'])
@login_required
def delete_schedule():
    """
    刪除單一班表（schedule）資料，需提供主鍵（id）。
    僅限管理員。
    """
    if session.get('role') != 'admin':
        flash('權限不足，僅限管理員操作', 'danger')
        return redirect(url_for('view_schedule'))
    schedule_id = request.form['id']
    conn = get_db_connection()
    before = conn.execute('SELECT * FROM schedule WHERE id = ?', (schedule_id,)).fetchone()
    if not before:
        flash('找不到班表資料', 'danger')
        conn.close()
        return redirect(url_for('view_schedule'))
    before_data = dict(before)
    conn.execute('DELETE FROM schedule WHERE id = ?', (schedule_id,))
    conn.execute('INSERT INTO schedule_log (schedule_id, action, before_data, after_data, operator_id, operated_at) VALUES (?, ?, ?, ?, ?, ?)',
                 (schedule_id, 'delete', json.dumps(before_data, ensure_ascii=False), None, session.get('username', 'system'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    flash('班表已刪除', 'success')
    return redirect(url_for('view_schedule'))

@app.route('/schedule_log')
@login_required
@admin_required
def schedule_log():
    conn = get_db_connection()
    logs = conn.execute('SELECT * FROM schedule_log ORDER BY created_at DESC LIMIT 100').fetchall()
    conn.close()
    return render_template('schedule_log.html', logs=logs)

# 新增：排班偏好設定頁面
@app.route('/staff_preference')
@login_required
@admin_required
def staff_preference():
    conn = get_db_connection()
    # 取得所有偏好設定
    preferences = conn.execute('''
        SELECT sp.*, s.name as staff_name, 
               sh1.name as shift_name_1, sh2.name as shift_name_2
        FROM staff_preference sp
        JOIN staff s ON sp.staff_id = s.staff_id
        JOIN shift sh1 ON sp.shift_id_1 = sh1.shift_id
        LEFT JOIN shift sh2 ON sp.shift_id_2 = sh2.shift_id
        ORDER BY sp.month DESC, s.staff_id
    ''').fetchall()
    
    staff_list = conn.execute('SELECT * FROM staff ORDER BY staff_id').fetchall()
    shift_list = conn.execute('SELECT * FROM shift ORDER BY name').fetchall()
    conn.close()
    
    today = datetime.today()
    default_month = today.strftime('%Y-%m')
    
    return render_template('staff_preference.html', 
                         preferences=preferences, 
                         staff_list=staff_list, 
                         shift_list=shift_list,
                         default_month=default_month)

# 新增：新增排班偏好
@app.route('/add_staff_preference', methods=['POST'])
@login_required
@admin_required
def add_staff_preference():
    month = request.form['month']
    staff_id = request.form['staff_id']
    preference_type = request.form['preference_type']
    shift_id_1 = request.form['shift_id_1']
    shift_id_2 = request.form.get('shift_id_2', None)
    week_pattern = request.form.get('week_pattern', None)
    
    conn = get_db_connection()
    try:
        # 檢查是否已存在該月份的偏好設定
        existing = conn.execute('SELECT id FROM staff_preference WHERE staff_id = ? AND month = ?', 
                               (staff_id, month)).fetchone()
        
        if existing:
            # 更新現有設定
            conn.execute('''UPDATE staff_preference 
                           SET preference_type = ?, shift_id_1 = ?, shift_id_2 = ?, week_pattern = ?
                           WHERE staff_id = ? AND month = ?''',
                       (preference_type, shift_id_1, shift_id_2, week_pattern, staff_id, month))
        else:
            # 新增設定
            conn.execute('''INSERT INTO staff_preference 
                           (staff_id, month, preference_type, shift_id_1, shift_id_2, week_pattern)
                           VALUES (?, ?, ?, ?, ?, ?)''',
                       (staff_id, month, preference_type, shift_id_1, shift_id_2, week_pattern))
        
        conn.commit()
        flash('排班偏好設定已儲存', 'success')
    except Exception as e:
        flash(f'儲存失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('staff_preference'))

# 新增：刪除排班偏好
@app.route('/delete_staff_preference', methods=['POST'])
@login_required
@admin_required
def delete_staff_preference():
    pref_id = request.form['id']
    conn = get_db_connection()
    conn.execute('DELETE FROM staff_preference WHERE id = ?', (pref_id,))
    conn.commit()
    conn.close()
    flash('排班偏好設定已刪除', 'success')
    return redirect(url_for('staff_preference'))

# 新增：假日 On Call 管理頁面
@app.route('/oncall_manage')
@login_required
@admin_required
def oncall_manage():
    conn = get_db_connection()
    staff_list = conn.execute('SELECT * FROM staff ORDER BY staff_id').fetchall()
    conn.close()
    
    today = datetime.today()
    default_month = today.strftime('%Y-%m')
    current_month = request.args.get('month', default_month)
    
    # 產生月曆資料
    calendar_days = generate_calendar_days(current_month)
    
    return render_template('oncall_manage.html', 
                         staff_list=staff_list,
                         default_month=default_month,
                         current_month=current_month,
                         calendar_days=calendar_days)

# 新增：新增 On Call 設定
@app.route('/add_oncall', methods=['POST'])
@login_required
@admin_required
def add_oncall():
    date = request.form['date']
    staff_id = request.form['staff_id']
    status = request.form['status']
    
    conn = get_db_connection()
    try:
        # 檢查是否已存在該日期的設定
        existing = conn.execute('SELECT id FROM oncall_schedule WHERE date = ? AND staff_id = ?', 
                               (date, staff_id)).fetchone()
        
        if existing:
            # 更新現有設定
            conn.execute('UPDATE oncall_schedule SET status = ? WHERE date = ? AND staff_id = ?',
                       (status, date, staff_id))
        else:
            # 新增設定
            conn.execute('INSERT INTO oncall_schedule (date, staff_id, status) VALUES (?, ?, ?)',
                       (date, staff_id, status))
        
        conn.commit()
        flash('On Call 設定已儲存', 'success')
    except Exception as e:
        flash(f'儲存失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('oncall_manage'))

# 新增：批次設定 On Call
@app.route('/batch_oncall', methods=['POST'])
@login_required
@admin_required
def batch_oncall():
    month = request.form['month']
    auto_weekend = request.form['auto_weekend']
    oncall_days = int(request.form['oncall_days'])
    
    conn = get_db_connection()
    try:
        # 取得該月所有週末日期
        if auto_weekend == 'yes':
            weekend_dates = get_weekend_dates(month)
            
            # 取得所有人員
            staff_list = conn.execute('SELECT staff_id FROM staff').fetchall()
            
            # 為每個週末分配 On Call 人員
            for i, date in enumerate(weekend_dates):
                staff_index = i % len(staff_list)
                staff_id = staff_list[staff_index]['staff_id']
                
                # 檢查是否已存在
                existing = conn.execute('SELECT id FROM oncall_schedule WHERE date = ? AND staff_id = ?', 
                                       (date, staff_id)).fetchone()
                
                if not existing:
                    conn.execute('INSERT INTO oncall_schedule (date, staff_id, status) VALUES (?, ?, ?)',
                               (date, staff_id, 'oncall'))
        
        conn.commit()
        flash('批次 On Call 設定已完成', 'success')
    except Exception as e:
        flash(f'批次設定失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('oncall_manage'))

# 輔助函數：產生月曆資料
def generate_calendar_days(month_str):
    year, month = map(int, month_str.split('-'))
    _, last_day = monthrange(year, month)
    
    calendar_days = []
    for day in range(1, last_day + 1):
        date = f"{year:04d}-{month:02d}-{day:02d}"
        weekday = datetime(year, month, day).strftime('%A')
        weekday_cn = {'Monday': '一', 'Tuesday': '二', 'Wednesday': '三', 
                     'Thursday': '四', 'Friday': '五', 'Saturday': '六', 'Sunday': '日'}[weekday]
        
        # 取得該日期的 On Call 人員
        conn = get_db_connection()
        oncall_staff = conn.execute('''
            SELECT ocs.*, s.name as staff_name
            FROM oncall_schedule ocs
            JOIN staff s ON ocs.staff_id = s.staff_id
            WHERE ocs.date = ?
        ''', (date,)).fetchall()
        conn.close()
        
        calendar_days.append({
            'date': date,
            'weekday': weekday_cn,
            'is_weekend': weekday in ['Saturday', 'Sunday'],
            'oncall_staff': oncall_staff
        })
    
    return calendar_days

# 輔助函數：取得週末日期
def get_weekend_dates(month_str):
    year, month = map(int, month_str.split('-'))
    _, last_day = monthrange(year, month)
    
    weekend_dates = []
    for day in range(1, last_day + 1):
        date = datetime(year, month, day)
        if date.strftime('%A') in ['Saturday', 'Sunday']:
            weekend_dates.append(date.strftime('%Y-%m-%d'))
    
    return weekend_dates

@app.route('/weekly_stats')
@login_required
def weekly_stats():
    month = request.args.get('month')
    if not month:
        today = datetime.today()
        month = today.strftime('%Y-%m')
    
    conn = get_db_connection()
    
    # 取得週工時統計
    stats = conn.execute('''
        SELECT wws.*, s.name as staff_name, s.staff_id
        FROM weekly_work_stats wws
        JOIN staff s ON wws.staff_id = s.staff_id
        WHERE wws.month = ?
        ORDER BY s.staff_id, wws.week_number
    ''', (month,)).fetchall()
    
    # 取得四周變形工時設定
    config = conn.execute('SELECT * FROM work_schedule_config WHERE month = ?', (month,)).fetchone()
    
    conn.close()
    
    return render_template('weekly_stats.html', stats=stats, config=config, month=month)

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)