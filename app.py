from flask import Flask, render_template, redirect, url_for, request, send_file, flash, session, jsonify
import os
import json
import csv
import math
from io import StringIO, BytesIO
import sqlite3
import random
from datetime import datetime, timedelta
from calendar import monthrange
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps

app = Flask(__name__)
app.secret_key = 'nurse-secret-key'  # session 用

def validate_schedule_requirements(dates, staff_list, shifts, night_shift_allocations, total_weeks):
    """
    驗證排班結果是否符合需求
    返回 (is_valid, validation_results)
    """
    conn = get_db_connection()
    validation_results = {
        'night_shift_priority': {'passed': True, 'details': []},
        'rest_days_arrangement': {'passed': True, 'details': []},
        'weekly_shift_consistency': {'passed': True, 'details': []},
        'overall_passed': True
    }
    
    try:
        # 1. 檢查大夜班預先分配是否優先安排
        for date in dates:
            night_allocations = night_shift_allocations.get(date, [])
            if night_allocations:
                for staff_id, allocated_shift_id in night_allocations:
                    # 檢查該員工在該日期是否確實被分配到預先指定的大夜班
                    actual_assignment = conn.execute('''
                        SELECT COUNT(*) as count FROM schedule 
                        WHERE date = ? AND staff_id = ? AND shift_id = ?
                    ''', (date, staff_id, allocated_shift_id)).fetchone()
                    
                    if actual_assignment['count'] == 0:
                        validation_results['night_shift_priority']['passed'] = False
                        validation_results['night_shift_priority']['details'].append(
                            f"預先分配失效：{date} 員工 {staff_id} 未被分配到指定大夜班 {allocated_shift_id}"
                        )
        
        # 2. 檢查每人每週休息日和例假日安排
        for staff in staff_list:
            staff_id = staff['staff_id']
            
            for week_num in range(1, total_weeks + 1):
                start_idx = (week_num - 1) * 7
                end_idx = min(week_num * 7, len(dates))
                week_dates = dates[start_idx:end_idx]
                
                if not week_dates:
                    continue
                
                # 檢查週日例假日
                sunday_count = 0
                rest_day_count = 0
                work_days = []
                
                for date in week_dates:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    is_sunday = date_obj.weekday() == 6
                    
                    # 檢查該員工在該日期是否有排班
                    work_assignment = conn.execute('''
                        SELECT shift.name FROM schedule 
                        JOIN shift ON schedule.shift_id = shift.shift_id
                        WHERE schedule.date = ? AND schedule.staff_id = ?
                    ''', (date, staff_id)).fetchall()
                    
                    if work_assignment:
                        work_days.append((date, [row['name'] for row in work_assignment]))
                        if is_sunday:
                            # 週日應該只有大夜班，其他班別不應該排班
                            non_night_shifts = [shift for shift in work_assignment if '大夜' not in shift['name']]
                            if non_night_shifts:
                                validation_results['rest_days_arrangement']['passed'] = False
                                validation_results['rest_days_arrangement']['details'].append(
                                    f"週日例假日違規：第{week_num}週 {date} 員工 {staff_id} 被排非大夜班 {[s['name'] for s in non_night_shifts]}"
                                )
                    else:
                        if is_sunday:
                            sunday_count += 1
                        else:
                            rest_day_count += 1
                
                # 檢查是否至少有一天休息日（週一到週六）
                if rest_day_count == 0 and len([d for d in week_dates if datetime.strptime(d, '%Y-%m-%d').weekday() < 6]) > 0:
                    validation_results['rest_days_arrangement']['passed'] = False
                    validation_results['rest_days_arrangement']['details'].append(
                        f"休息日不足：第{week_num}週 員工 {staff_id} 沒有平日休息日"
                    )
        
        # 3. 檢查每人每週班別種類（最多兩種）
        for staff in staff_list:
            staff_id = staff['staff_id']
            
            for week_num in range(1, total_weeks + 1):
                start_idx = (week_num - 1) * 7
                end_idx = min(week_num * 7, len(dates))
                week_dates = dates[start_idx:end_idx]
                
                if not week_dates:
                    continue
                
                # 統計該週班別種類
                week_shifts = set()
                for date in week_dates:
                    shifts_on_date = conn.execute('''
                        SELECT shift.shift_id, shift.name FROM schedule 
                        JOIN shift ON schedule.shift_id = shift.shift_id
                        WHERE schedule.date = ? AND schedule.staff_id = ?
                    ''', (date, staff_id)).fetchall()
                    
                    for shift in shifts_on_date:
                        week_shifts.add(shift['shift_id'])
                
                # 檢查班別種類是否超過 2 種
                if len(week_shifts) > 2:
                    shift_names = []
                    for shift_id in week_shifts:
                        shift_name = conn.execute('SELECT name FROM shift WHERE shift_id = ?', (shift_id,)).fetchone()
                        if shift_name:
                            shift_names.append(shift_name['name'])
                    
                    validation_results['weekly_shift_consistency']['passed'] = False
                    validation_results['weekly_shift_consistency']['details'].append(
                        f"班別種類過多：第{week_num}週 員工 {staff_id} 被安排 {len(week_shifts)} 種班別 {shift_names}"
                    )
        
        # 設定整體驗證結果
        validation_results['overall_passed'] = (
            validation_results['night_shift_priority']['passed'] and
            validation_results['rest_days_arrangement']['passed'] and
            validation_results['weekly_shift_consistency']['passed']
        )
        
    except Exception as e:
        validation_results['overall_passed'] = False
        validation_results['error'] = str(e)
    
    finally:
        conn.close()
    
    return validation_results['overall_passed'], validation_results

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
    
    # 新增：請假管理表
    c.execute('''CREATE TABLE IF NOT EXISTS leave_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        staff_id TEXT NOT NULL,
        leave_type TEXT NOT NULL,  -- 請假假別：事假、病假、特休、婚假、喪假、產假、陪產假、其他
        start_date TEXT NOT NULL,  -- 起始日期 YYYY-MM-DD
        end_date TEXT NOT NULL,    -- 結束日期 YYYY-MM-DD
        reason TEXT,               -- 請假原因（可選）
        approved BOOLEAN DEFAULT 1, -- 是否核准（預設核准）
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        operator_id TEXT,          -- 操作者
        FOREIGN KEY (staff_id) REFERENCES staff (staff_id)
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
        
        # 3. 建立大夜班預先分配表
        conn.execute('''CREATE TABLE IF NOT EXISTS night_shift_allocation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            staff_id TEXT NOT NULL,
            shift_id TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
            FOREIGN KEY (shift_id) REFERENCES shift (shift_id),
            UNIQUE(start_date, end_date, staff_id)
        )''')
        
        # 如果舊表存在，進行資料遷移
        try:
            # 檢查是否有舊的表結構
            old_columns = conn.execute("PRAGMA table_info(night_shift_allocation)").fetchall()
            has_old_structure = any(col[1] == 'week_number' for col in old_columns)
            
            if has_old_structure:
                # 備份舊資料
                old_data = conn.execute("SELECT * FROM night_shift_allocation").fetchall()
                
                # 刪除舊表
                conn.execute("DROP TABLE night_shift_allocation")
                
                # 重新建立新表
                conn.execute('''CREATE TABLE night_shift_allocation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    staff_id TEXT NOT NULL,
                    shift_id TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
                    FOREIGN KEY (shift_id) REFERENCES shift (shift_id),
                    UNIQUE(start_date, end_date, staff_id)
                )''')
                
                print("大夜班分配表結構已更新為日期範圍模式")
        except:
            pass
        
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
    
    # 轉換為列表並處理每日需求人數
    processed_shifts = []
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
        
        processed_shifts.append(shift_dict)
    
    conn.close()
    return render_template('shift.html', shift_list=processed_shifts)

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
        'start_date': '',
        'end_date': '',
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
        filters['start_date'] = request.form.get('start_date', '')
        filters['end_date'] = request.form.get('end_date', '')
        filters['shift_name'] = request.form.get('shift_name', '')
        filters['ward'] = request.form.get('ward', '')
        filters['staff_name'] = request.form.get('staff_name', '')
        
        if filters['date']:
            query += ' AND schedule.date = ?'
            params.append(filters['date'])
        elif filters['start_date'] and filters['end_date']:
            query += ' AND schedule.date BETWEEN ? AND ?'
            params.extend([filters['start_date'], filters['end_date']])
        
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
    
    # 計算班數統計（支援日期範圍）
    staff_stats = []
    if schedule:
        staff_count = {}
        staff_name_map = {}
        for row in schedule:
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

   # ---------- 自動偵測：有 start/end 就用範圍，否則用月模式 ----------
    start_raw = request.form.get('start_date')
    end_raw   = request.form.get('end_date')
    if start_raw and end_raw:
        # 使用自訂範圍
        schedule_mode = 'range'
    else:
        # 沒傳就用月模式
        schedule_mode = 'month'

    # ---------- 統一取得 start_date_obj / end_date_obj ----------
    if schedule_mode == 'month':
        raw_month = request.form.get('month')
        if not raw_month:
            today = datetime.today()
            year, mon = today.year, today.month
        else:
            year, mon = map(int, raw_month.split('-'))
        start_date_obj = datetime(year, mon, 1)
        end_date_obj   = datetime(year, mon, monthrange(year, mon)[1])
    else:
        # 自訂範圍
        if not start_raw or not end_raw:
            flash('請選擇起始日期與結束日期', 'danger')
            return redirect(url_for('schedule'))
        start_date_obj = datetime.strptime(start_raw, '%Y-%m-%d')
        end_date_obj   = datetime.strptime(end_raw,   '%Y-%m-%d')
        if start_date_obj > end_date_obj:
            flash('起始日期不能大於結束日期', 'danger')
            return redirect(url_for('schedule'))
        if (end_date_obj - start_date_obj).days > 365:
            flash('排班日期範圍不能超過一年', 'danger')
            return redirect(url_for('schedule'))
        
    # ---------- 統一產生 dates & months 清單 ----------
    dates = []
    cur = start_date_obj
    while cur <= end_date_obj:
        dates.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)
    # 去重並排序月份（格式 YYYY-MM）
    months = sorted({d[:7] for d in dates})

    # ---------- 計算總週數 ----------
    total_weeks = math.ceil(len(dates) / 7)

    # ---------- 讀取其他排班參數 ----------
    max_per_day           = int(request.form.get('max_per_day', 1))
    max_consecutive       = int(request.form.get('max_consecutive', 5))
    min_per_month         = int(request.form.get('min_per_month', 22))
    max_per_month         = int(request.form.get('max_per_month', 30))
    max_night_consecutive = int(request.form.get('max_night_consecutive', 2))
    max_night_per_month   = int(request.form.get('max_night_per_month', 8))
    auto_fill_missing     = (request.form.get('auto_fill_missing', 'yes') == 'yes')
    fair_distribution     = (request.form.get('fair_distribution', 'yes') == 'yes')
    special_preference    = (request.form.get('special_preference', 'no') == 'yes')
    
    # 四周變形工時參數
    is_flexible_workweek = (request.form.get('is_flexible_workweek', 'yes') == 'yes')
    require_holiday       = (request.form.get('require_holiday',      'yes') == 'yes')
    require_rest_day      = (request.form.get('require_rest_day',     'yes') == 'yes')
    holiday_day           = int(request.form.get('holiday_day', 7))  # 預設週日
    
    # 週班別一致性參數
    week_shift_consistency = (request.form.get('week_shift_consistency', 'yes') == 'yes')

    # ---------- 資料庫連線 & 儲存配置 ----------
    conn = get_db_connection()
    # 只用第一個月份作為配置 key
    conn.execute(
        '''INSERT OR REPLACE INTO work_schedule_config
                    (month, is_flexible_workweek, require_holiday, require_rest_day, holiday_day) 
                    VALUES (?, ?, ?, ?, ?)''', 
        (months[0], is_flexible_workweek, require_holiday, require_rest_day, holiday_day)
    )
    
    # 讀取排班班別與員工
    shifts = conn.execute('SELECT * FROM shift').fetchall()
    staff  = conn.execute('SELECT * FROM staff').fetchall()
    staff_list = [dict(s) for s in staff]
    
    # 建立每日需求人數字典
    daily_requirements = {}
    for shift in shifts:
        sid = shift['shift_id']
        daily_requirements[sid] = {}
        for dow in range(1, 8):
            req = conn.execute(
                'SELECT required_count FROM shift_daily_requirements WHERE shift_id = ? AND day_of_week = ?', 
                (sid, dow)
            ).fetchone()
            daily_requirements[sid][dow] = req['required_count'] if req else shift['required_count']
    # ---------- 初始化員工狀態（務必放在這裡） ----------
    staff_status = {
        s['staff_id']: {
            'count':           0,
            'consecutive':     0,
            'last_date':       None,
            'last_worked':     False,
            'shift_counts':    {},
            'night_count':     0,
            'night_consecutive': 0,
            'last_night_date':   None,
            'last_night_worked': False,
            'weekly_hours':    {w: 0 for w in range(1, total_weeks + 1)},
            'holiday_days':    {w: 0 for w in range(1, total_weeks + 1)},
            'rest_days':       {w: 0 for w in range(1, total_weeks + 1)},
            'worked_days':     {w: 0 for w in range(1, total_weeks + 1)},
            'weekly_shifts':   {w: set() for w in range(1, total_weeks + 1)},  # 追蹤每週的班別
        }
        for s in staff_list
    }
    # 動態加入 weekX_count
    for sid in staff_status:
        for w in range(1, total_weeks + 1):
            staff_status[sid][f'week{w}_count'] = 0

    # --------- 讀取特殊偏好（支援多個月份） ----------
    preferences = {}  # key: (staff_id, month), value: pref 資料
    if special_preference:
        # 產生 (?, ?, ..., ?) 的字串
        placeholder = ','.join('?' for _ in months)
        sql = f"SELECT * FROM staff_preference WHERE month IN ({placeholder})"
        rows = conn.execute(sql, months).fetchall()
        for p in rows:
            # 以 (staff_id, month) 當 key
            preferences[(p['staff_id'], p['month'])] = {
                'type':         p['preference_type'],
                'shift_id_1':   p['shift_id_1'],
                'shift_id_2':   p['shift_id_2'],
                'week_pattern': p['week_pattern'],
            }

    # --------- 讀取大夜班預先分配 ----------
    night_shift_allocations = {}  # key: date, value: [(staff_id, shift_id), ...]
    
    # 取得日期範圍內的所有大夜班分配
    start_date = dates[0] if dates else None
    end_date = dates[-1] if dates else None
    
    if start_date and end_date:
        allocations = conn.execute('''
            SELECT * FROM night_shift_allocation 
            WHERE (start_date <= ? AND end_date >= ?) OR 
                  (start_date >= ? AND start_date <= ?) OR
                  (end_date >= ? AND end_date <= ?)
            ORDER BY start_date, staff_id
        ''', (end_date, start_date, start_date, end_date, start_date, end_date)).fetchall()
        
        for allocation in allocations:
            alloc_start = allocation['start_date']
            alloc_end = allocation['end_date']
            staff_id = allocation['staff_id']
            shift_id = allocation['shift_id']
            
            # 為該分配範圍內的每一天建立記錄
            current_date = datetime.strptime(alloc_start, '%Y-%m-%d')
            end_date_obj = datetime.strptime(alloc_end, '%Y-%m-%d')
            
            while current_date <= end_date_obj:
                date_str = current_date.strftime('%Y-%m-%d')
                
                # 只處理在排班日期範圍內的日期
                if date_str in dates:
                    if date_str not in night_shift_allocations:
                        night_shift_allocations[date_str] = []
                    night_shift_allocations[date_str].append((staff_id, shift_id))
                
                current_date += timedelta(days=1)

    # ---------- 清除舊排班 ----------
    for m in months:
        conn.execute('DELETE FROM schedule           WHERE date LIKE ?', (f"{m}%",))
        conn.execute('DELETE FROM weekly_work_stats WHERE month = ?',     (m,))

    now_str  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    operator = session.get('username', 'system')
    


    # ---------- 計算每週例假與休息日 ----------
    staff_holidays = {s['staff_id']: {} for s in staff_list}
    staff_restdays = {s['staff_id']: {} for s in staff_list}
    
    # 例假（週日固定）
    for sid in staff_holidays:
        for w in range(1, total_weeks + 1):  # 需要縮排
            start_idx = (w-1)*7
            end_idx = min(w*7, len(dates))
            week_dates = dates[start_idx : end_idx]
            for d in week_dates:
                if datetime.strptime(d, '%Y-%m-%d').weekday() == 6:
                    staff_holidays[sid][w] = d
                    break

    # 休息日（週一到週六隨機）
    for sid in staff_restdays:
        for w in range(1, total_weeks + 1):
            start_idx = (w-1)*7
            end_idx = min(w*7, len(dates))  # 避免超出範圍
            week_dates = dates[start_idx : end_idx]
            choices = [d for d in week_dates if datetime.strptime(d, '%Y-%m-%d').weekday() < 6]
            if choices:
                staff_restdays[sid][w] = random.choice(choices)

    # ---------- 主迴圈：每日排班 ----------
    for idx, date in enumerate(dates):
        date_obj     = datetime.strptime(date, '%Y-%m-%d')
        week_of_month = (idx // 7) + 1
        dow           = date_obj.weekday() + 1
        is_holiday    = (dow == holiday_day)
        worked_today  = set()

        # ---------- 星期日 On Call 處理 ----------
        if dow == 7:  # 星期日（只有星期天安排 On Call）
            # 檢查是否已有 On Call 設定
            existing_oncall = conn.execute(
                'SELECT staff_id FROM oncall_schedule WHERE date = ?', (date,)
            ).fetchone()
            
            if not existing_oncall:
                # 計算每位員工的 On Call 次數（本月）
                current_month = date[:7]
                oncall_counts = {}
                for s in staff_list:
                    # 檢查該員工是否在此星期天請假
                    leave_check = conn.execute('''
                        SELECT COUNT(*) FROM leave_schedule 
                        WHERE staff_id = ? AND start_date <= ? AND end_date >= ? AND approved = 1
                    ''', (s['staff_id'], date, date)).fetchone()[0]
                    
                    if leave_check == 0:  # 沒有請假才納入 On Call 候選
                        count = conn.execute(
                            'SELECT COUNT(*) FROM oncall_schedule WHERE staff_id = ? AND date LIKE ?',
                            (s['staff_id'], f"{current_month}%")
                        ).fetchone()[0]
                        oncall_counts[s['staff_id']] = count
                
                # 選擇 On Call 次數最少的員工（排除請假員工）
                if oncall_counts:
                    min_count = min(oncall_counts.values())
                    candidates = [s for s in staff_list if oncall_counts.get(s['staff_id'], float('inf')) == min_count]
                    
                    if candidates:
                        oncall_staff = random.choice(candidates)
                        conn.execute(
                            'INSERT INTO oncall_schedule (date, staff_id, status) VALUES (?, ?, ?)',
                            (date, oncall_staff['staff_id'], 'oncall')
                        )
                        print(f"自動設定 {date} 星期日 On Call: {oncall_staff['name']} (本月第{min_count+1}次)")
                    else:
                        print(f"警告：{date} 星期日無法安排 On Call，所有員工都在請假")
                else:
                    print(f"警告：{date} 星期日無法安排 On Call，所有員工都在請假")

        # 重新排序班別：大夜班優先處理（特別是有預先分配的）
        shifts_ordered = []
        night_shifts_with_allocation = []
        other_shifts = []
        
        for shift in shifts:
            is_night = '大夜' in shift['name']
            if is_night and date in night_shift_allocations:
                # 有預先分配的大夜班最優先
                night_shifts_with_allocation.append(shift)
            elif is_night:
                # 其他大夜班次之
                other_shifts.insert(0, shift)
            else:
                # 非大夜班最後
                other_shifts.append(shift)
        
        shifts_ordered = night_shifts_with_allocation + other_shifts
        
        # Debug 輸出班別處理順序
        if night_shifts_with_allocation:
            shift_names = [s['name'] for s in night_shifts_with_allocation]
            print(f"🌙 {date} 優先處理有預先分配的大夜班: {', '.join(shift_names)}")

        for shift in shifts_ordered:
            sid_shift  = shift['shift_id']
            required   = daily_requirements[sid_shift][dow]
            ward       = shift['ward']
            is_night   = '大夜' in shift['name']
            candidates = []
            
            # 檢查是否有大夜班預先分配
            night_allocations = night_shift_allocations.get(date, [])
            pre_allocated_staff_ids = set()  # 記錄已預先分配的員工ID
            
            # 如果是大夜班且有預先分配，強制優先使用預先分配的員工
            if is_night and night_allocations:
                print(f"📋 {date} {shift['name']} 檢查預先分配: {len(night_allocations)} 筆分配")
                for staff_id, allocated_shift_id in night_allocations:
                    if allocated_shift_id == sid_shift:
                        # 找到對應的員工
                        allocated_staff = next((s for s in staff_list if s['staff_id'] == staff_id), None)
                        if allocated_staff and allocated_staff['ward'] == ward:
                            st = staff_status[staff_id]
                            
                            # 預先分配的員工只做基本檢查，放寬大部分限制
                            if st['shift_counts'].get(date, 0) < max_per_day:  # 只檢查當日是否已排班
                                candidates.append((allocated_staff, st['count'], st['shift_counts'].get(sid_shift, 0), -1))  # -1 表示預先分配最優先
                                pre_allocated_staff_ids.add(staff_id)
                                print(f"使用大夜班預先分配：{date} {shift['name']} -> {allocated_staff['name']}")
                            else:
                                print(f"警告：預先分配員工 {allocated_staff['name']} 在 {date} 已經排班，跳過")
                
                # 如果有預先分配且找到足夠人數，直接使用預先分配，不再篩選其他員工
                if len(candidates) >= required:
                    print(f"大夜班預先分配已滿足需求：{date} {shift['name']} 需要{required}人，已分配{len(candidates)}人")
                    assigned = candidates[:required]
                    for c in assigned:
                        s   = c[0]
                        sid = s['staff_id']
                        st  = staff_status[sid]

                        # 更新狀態
                        st['count'] += 1
                        st['shift_counts'][sid_shift] = st['shift_counts'].get(sid_shift, 0) + 1
                        st['shift_counts'][date]     = st['shift_counts'].get(date, 0)     + 1
                        st['last_date']   = date
                        st['last_worked'] = True
                        st[f'week{week_of_month}_count'] += 1
                        st['weekly_hours'][week_of_month] += 8
                        if is_holiday:
                            st['holiday_days'][week_of_month] += 1
                        st['worked_days'][week_of_month] += 1
                        
                        # 更新週班別追蹤
                        st['weekly_shifts'][week_of_month].add(sid_shift)

                        # 寫入 schedule
                        conn.execute(
                            '''INSERT INTO schedule
                               (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                            (date, sid_shift, sid, 8, 1, operator, now_str, now_str)
                        )
                        worked_today.add(sid)
                    
                    # 大夜班預先分配已完成，跳到下一個班別
                    continue
                        
                        # 篩選可用員工（排除已預先分配的員工）
            for s in staff_list:
                sid = s['staff_id']
                if s['ward'] != ward:
                    continue
                # 跳過已經在預先分配中的員工，避免重複
                if sid in pre_allocated_staff_ids:
                    continue
                st = staff_status[sid]

                # 🚨 第一優先：請假檢查 - 如果該員工在此日期請假，則跳過
                leave_check = conn.execute('''
                    SELECT COUNT(*) FROM leave_schedule 
                    WHERE staff_id = ? AND start_date <= ? AND end_date >= ? AND approved = 1
                ''', (sid, date, date)).fetchone()[0]
                
                if leave_check > 0:
                    continue  # 該員工在此日期有請假，跳過

                # 偏好檢查 - 根據日期月份查找偏好設定
                date_month = date[:7]  # 取得日期的年-月部分
                pref = preferences.get((sid, date_month))
                if pref:
                    if pref['type'] == 'single':
                        if sid_shift != pref['shift_id_1']:
                            continue
                    elif pref['type'] == 'dual':
                        if sid_shift not in [pref['shift_id_1'], pref['shift_id_2']]:
                            continue
                        if pref['week_pattern'] == 'alternate':
                            if week_of_month % 2 == 1:
                                if sid_shift != pref['shift_id_1']:
                                    continue
                            else:
                                if sid_shift != pref['shift_id_2']:
                                    continue
                        elif pref['week_pattern'] == 'consecutive':
                            main_shift_count = st['shift_counts'].get(pref['shift_id_1'], 0)
                            secondary_shift_count = st['shift_counts'].get(pref['shift_id_2'], 0)
                            if main_shift_count <= secondary_shift_count:
                                if sid_shift != pref['shift_id_1']:
                                    continue
                            else:
                                if sid_shift != pref['shift_id_2']:
                                    continue
                
                                # 節假日與休息日檢查
                if not is_night:
                    if staff_holidays[sid].get(week_of_month) == date:
                        continue
                    if staff_restdays[sid].get(week_of_month) == date:
                        continue

                # 次數、工時、間隔等檢查（與原邏輯相同）
                if st['count'] >= max_per_month:
                    continue
                if st['shift_counts'].get(date, 0) >= max_per_day:
                    continue
                if is_flexible_workweek:
                    if st['weekly_hours'][week_of_month] >= 40:
                        continue
                    if require_holiday and is_holiday and st['holiday_days'][week_of_month] > 0:
                        continue

                # 週班別一致性評分
                week_consistency_score = 0
                if week_shift_consistency:
                    current_week_shifts = st['weekly_shifts'][week_of_month]
                    if current_week_shifts:
                        # 如果本週已有班別，相同班別優先
                        if sid_shift in current_week_shifts:
                            week_consistency_score = 0  # 最高優先級
                        else:
                            week_consistency_score = 1  # 較低優先級（不同班別）
                    else:
                        week_consistency_score = 0  # 本週還沒排班，所有班別平等

                candidates.append((s, st['count'], st['shift_counts'].get(sid_shift, 0), week_consistency_score))

            # 排序候選人
            if fair_distribution:
                if week_shift_consistency:
                    # 考慮週班別一致性的排序：預先分配 > 偏好設定 > 週班別一致性 > 總班數 > 該班別次數
                    candidates.sort(key=lambda c: (
                        0 if c[3] == -1 else 1,  # 預先分配最優先
                        0 if preferences.get((c[0]['staff_id'], date_month)) else 1,  # 偏好設定優先
                        c[3] if c[3] != -1 else 0,  # 週班別一致性評分
                        c[1],  # 總班數
                        c[2]   # 該班別次數
                    ))
                else:
                    candidates.sort(key=lambda c: (
                        0 if c[3] == -1 else 1,  # 預先分配最優先
                        0 if preferences.get((c[0]['staff_id'], date_month)) else 1, 
                        c[1], 
                        c[2]
                    ))
            else:
                # 保留原本隨機但分組邏輯，但預先分配仍然優先
                pre_allocated = [c for c in candidates if c[3] == -1]
                others = [c for c in candidates if c[3] != -1]
                random.shuffle(others)
                candidates = pre_allocated + others

            # 指派
            assigned = candidates[:required]
            for c in assigned:
                s   = c[0]
                sid = s['staff_id']
                st  = staff_status[sid]
                
                # 更新狀態
                st['count'] += 1
                st['shift_counts'][sid_shift] = st['shift_counts'].get(sid_shift, 0) + 1
                st['shift_counts'][date]     = st['shift_counts'].get(date, 0)     + 1
                st['last_date']   = date
                st['last_worked'] = True
                st[f'week{week_of_month}_count'] += 1
                st['weekly_hours'][week_of_month] += 8
                if is_holiday:
                    st['holiday_days'][week_of_month] += 1
                st['worked_days'][week_of_month] += 1
                
                # 更新週班別追蹤
                st['weekly_shifts'][week_of_month].add(sid_shift)
            
                # 寫入 schedule
                conn.execute(
                    '''INSERT INTO schedule
                       (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (date, sid_shift, sid, 8, 1, operator, now_str, now_str)
                )
                worked_today.add(sid)

            # 自動填補缺員
            if auto_fill_missing and len(assigned) < required:
                for _ in range(required - len(assigned)):
                    conn.execute(
                        '''INSERT INTO schedule
                           (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (date, sid_shift, '缺人值班', 8, 1, operator, now_str, now_str)
                    )
        
        # 當日未上班者累 rest_days
        if not is_holiday:
            for s in staff_list:
                sid = s['staff_id']
                if sid not in worked_today:
                    staff_status[sid]['rest_days'][week_of_month] += 1

    # ---------- 儲存週工時統計 ----------
    for sid, st in staff_status.items():
        for w in range(1, total_weeks + 1):
            # 決定該週的統計月份 - 根據實際日期計算
            start_idx = (w-1)*7
            end_idx = min(w*7, len(dates))
            week_dates = dates[start_idx : end_idx]
            
            if week_dates:
                # 使用該週第一個日期來判斷月份
                first_date = datetime.strptime(week_dates[0], '%Y-%m-%d')
                stats_month = first_date.strftime('%Y-%m')
            else:
                # 備用方案：使用第一個月份
                stats_month = months[0]
            
            conn.execute(
                '''INSERT INTO weekly_work_stats
                   (staff_id, month, week_number, total_hours, holiday_count, rest_day_count, work_days)
                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (
                    sid,
                    stats_month,
                    w,
                    st['weekly_hours'][w],
                    st['holiday_days'][w],
                    st['rest_days'][w],
                    st['worked_days'][w]
                )
            )

    conn.commit()
    conn.close()

    # 驗證排班結果是否符合需求
    print("🔍 開始驗證排班結果...")
    is_valid, validation_results = validate_schedule_requirements(
        dates, staff_list, shifts, night_shift_allocations, total_weeks
    )
    
    if is_valid:
        print("✅ 排班結果驗證通過！")
        return jsonify({
            'success': True,
            'message': '排班完成且符合所有需求！',
            'validation_results': validation_results,
            'redirect_url': url_for('calendar_view', month=months[0])
        })
    else:
        print("❌ 排班結果驗證失敗，準備重新生成...")
        return jsonify({
            'success': False,
            'message': '排班結果不符合需求，系統將自動重新生成',
            'validation_results': validation_results,
            'need_regenerate': True
        })

@app.route('/auto_schedule_with_validation', methods=['POST'])
@login_required
def auto_schedule_with_validation():
    """
    帶有驗證機制的自動排班，會重新生成直到符合需求
    """
    max_retries = 10  # 最大重試次數
    retry_count = 0
    
    while retry_count < max_retries:
        retry_count += 1
        print(f"🔄 第 {retry_count} 次排班嘗試...")
        
        # 執行自動排班邏輯（復用原有邏輯）
        try:
            # 讀取表單參數
            month = request.form.get('month', '')
            start_date = request.form.get('start_date', '')
            end_date = request.form.get('end_date', '')
            
            # 自動偵測模式
            if start_date and end_date:
                # 自訂日期範圍模式
                start_obj = datetime.strptime(start_date, '%Y-%m-%d')
                end_obj = datetime.strptime(end_date, '%Y-%m-%d')
                dates = []
                current_date = start_obj
                while current_date <= end_obj:
                    dates.append(current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
                months = list(set([d[:7] for d in dates]))
                total_weeks = math.ceil(len(dates) / 7)
            else:
                # 整月模式
                year, mon = map(int, month.split('-'))
                days_in_month = monthrange(year, mon)[1]
                dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
                months = [month]
                total_weeks = math.ceil(days_in_month / 7)
            
            # 其他參數
            max_per_day = int(request.form.get('max_per_day', 1))
            max_consecutive = int(request.form.get('max_consecutive', 5))
            min_per_month = int(request.form.get('min_per_month', 22))
            max_per_month = int(request.form.get('max_per_month', 30))
            max_night_consecutive = int(request.form.get('max_night_consecutive', 2))
            max_night_per_month = int(request.form.get('max_night_per_month', 8))
            auto_fill_missing = (request.form.get('auto_fill_missing', 'yes') == 'yes')
            fair_distribution = (request.form.get('fair_distribution', 'yes') == 'yes')
            special_preference = (request.form.get('special_preference', 'no') == 'yes')
            is_flexible_workweek = (request.form.get('is_flexible_workweek', 'yes') == 'yes')
            require_holiday = (request.form.get('require_holiday', 'yes') == 'yes')
            require_rest_day = (request.form.get('require_rest_day', 'yes') == 'yes')
            holiday_day = int(request.form.get('holiday_day', 7))
            week_shift_consistency = (request.form.get('week_shift_consistency', 'yes') == 'yes')
            
            # 強制啟用關鍵設定以提高通過驗證的機率
            week_shift_consistency = True  # 強制啟用週班別一致性
            require_holiday = True  # 強制啟用例假日
            require_rest_day = True  # 強制啟用休息日
            
            # 調用原有的自動排班邏輯（簡化版本，直接導向核心邏輯）
            result = execute_auto_schedule_logic(
                dates, months, total_weeks, max_per_day, max_consecutive,
                min_per_month, max_per_month, max_night_consecutive, max_night_per_month,
                auto_fill_missing, fair_distribution, special_preference,
                is_flexible_workweek, require_holiday, require_rest_day, holiday_day,
                week_shift_consistency
            )
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'message': f'排班成功！經過 {retry_count} 次嘗試找到符合需求的排班結果',
                    'retry_count': retry_count,
                    'validation_results': result['validation_results'],
                    'redirect_url': result['redirect_url']
                })
            
        except Exception as e:
            print(f"❌ 第 {retry_count} 次嘗試失敗：{str(e)}")
            if retry_count >= max_retries:
                return jsonify({
                    'success': False,
                    'message': f'排班失敗：嘗試 {max_retries} 次後仍無法生成符合需求的排班結果',
                    'error': str(e),
                    'retry_count': retry_count
                })
            continue
    
    return jsonify({
        'success': False,
        'message': f'排班失敗：超過最大重試次數 {max_retries}',
        'retry_count': retry_count
    })

def execute_auto_schedule_logic(dates, months, total_weeks, max_per_day, max_consecutive,
                              min_per_month, max_per_month, max_night_consecutive, max_night_per_month,
                              auto_fill_missing, fair_distribution, special_preference,
                              is_flexible_workweek, require_holiday, require_rest_day, holiday_day,
                              week_shift_consistency):
    """
    執行自動排班核心邏輯，返回結果字典
    這是對原有 auto_schedule 函數的簡化版本，專門用於驗證重新生成
    """
    import random
    
    conn = get_db_connection()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    operator = session.get('username', 'system')
    
    # 清除舊排班
    for m in months:
        conn.execute('DELETE FROM schedule WHERE date LIKE ?', (f"{m}%",))
        conn.execute('DELETE FROM weekly_work_stats WHERE month = ?', (m,))
    
    # 讀取資料
    shifts = conn.execute('SELECT * FROM shift').fetchall()
    staff = conn.execute('SELECT * FROM staff').fetchall()
    staff_list = [dict(s) for s in staff]
    
    # 建立每日需求人數字典
    daily_requirements = {}
    for shift in shifts:
        sid = shift['shift_id']
        daily_requirements[sid] = {}
        for dow in range(1, 8):
            req = conn.execute(
                'SELECT required_count FROM shift_daily_requirements WHERE shift_id = ? AND day_of_week = ?',
                (sid, dow)
            ).fetchone()
            daily_requirements[sid][dow] = req['required_count'] if req else shift['required_count']
    
    # 讀取大夜班預先分配
    night_shift_allocations = {}
    start_date = dates[0] if dates else None
    end_date = dates[-1] if dates else None
    
    if start_date and end_date:
        allocations = conn.execute('''
            SELECT * FROM night_shift_allocation 
            WHERE (start_date <= ? AND end_date >= ?) OR 
                  (start_date >= ? AND start_date <= ?) OR
                  (end_date >= ? AND end_date <= ?)
            ORDER BY start_date, staff_id
        ''', (end_date, start_date, start_date, end_date, start_date, end_date)).fetchall()
        
        for allocation in allocations:
            alloc_start = allocation['start_date']
            alloc_end = allocation['end_date']
            staff_id = allocation['staff_id']
            shift_id = allocation['shift_id']
            
            current_date = datetime.strptime(alloc_start, '%Y-%m-%d')
            end_date_obj = datetime.strptime(alloc_end, '%Y-%m-%d')
            
            while current_date <= end_date_obj:
                date_str = current_date.strftime('%Y-%m-%d')
                if date_str in dates:
                    if date_str not in night_shift_allocations:
                        night_shift_allocations[date_str] = []
                    night_shift_allocations[date_str].append((staff_id, shift_id))
                current_date += timedelta(days=1)
    
    # 簡化的排班邏輯：隨機分配但滿足基本約束
    staff_status = {
        s['staff_id']: {
            'count': 0,
            'shift_counts': {},
            'weekly_shifts': {w: set() for w in range(1, total_weeks + 1)},
            'weekly_hours': {w: 0 for w in range(1, total_weeks + 1)},
            'holiday_days': {w: 0 for w in range(1, total_weeks + 1)},
            'rest_days': {w: 0 for w in range(1, total_weeks + 1)},
            'worked_days': {w: 0 for w in range(1, total_weeks + 1)},
        }
        for s in staff_list
    }
    
    # 計算每週例假與休息日
    staff_holidays = {s['staff_id']: {} for s in staff_list}
    staff_restdays = {s['staff_id']: {} for s in staff_list}
    
    for sid in staff_holidays:
        for w in range(1, total_weeks + 1):  # 需要縮排
            start_idx = (w-1)*7
            end_idx = min(w*7, len(dates))
            week_dates = dates[start_idx : end_idx]
            # 週日例假
            for d in week_dates:
                if datetime.strptime(d, '%Y-%m-%d').weekday() == 6:
                    staff_holidays[sid][w] = d
                    break
            # 週一到週六隨機休息日
            choices = [d for d in week_dates if datetime.strptime(d, '%Y-%m-%d').weekday() < 6]
            if choices:
                staff_restdays[sid][w] = random.choice(choices)
    
    # 每日排班
    for idx, date in enumerate(dates):
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        week_of_month = (idx // 7) + 1
        dow = date_obj.weekday() + 1
        worked_today = set()
        
        # 班別處理順序：大夜班優先
        shifts_ordered = []
        night_shifts_with_allocation = []
        other_shifts = []
        
        for shift in shifts:
            is_night = '大夜' in shift['name']
            if is_night and date in night_shift_allocations:
                night_shifts_with_allocation.append(shift)
            elif is_night:
                other_shifts.insert(0, shift)
            else:
                other_shifts.append(shift)
        
        shifts_ordered = night_shifts_with_allocation + other_shifts
        
        for shift in shifts_ordered:
            sid_shift = shift['shift_id']
            required = daily_requirements[sid_shift][dow]
            ward = shift['ward']
            is_night = '大夜' in shift['name']
            candidates = []
            
            # 處理大夜班預先分配
            night_allocations = night_shift_allocations.get(date, [])
            pre_allocated_staff_ids = set()
            
            if is_night and night_allocations:
                for staff_id, allocated_shift_id in night_allocations:
                    if allocated_shift_id == sid_shift:
                        allocated_staff = next((s for s in staff_list if s['staff_id'] == staff_id), None)
                        if allocated_staff and allocated_staff['ward'] == ward:
                            st = staff_status[staff_id]
                            if st['shift_counts'].get(date, 0) < max_per_day:
                                candidates.append((allocated_staff, st['count'], st['shift_counts'].get(sid_shift, 0), -1))
                                pre_allocated_staff_ids.add(staff_id)
                
            # 如果預先分配已滿足需求，直接指派
            if len(candidates) >= required:
                assigned = candidates[:required]
            else:
                # 篩選其他可用員工
                for s in staff_list:
                    sid = s['staff_id']
                    if s['ward'] != ward or sid in pre_allocated_staff_ids:
                        continue
                    
                    st = staff_status[sid]
                    
                    # 🚨 第一優先：請假檢查 - 如果該員工在此日期請假，則跳過
                    leave_check = conn.execute('''
                        SELECT COUNT(*) FROM leave_schedule 
                        WHERE staff_id = ? AND start_date <= ? AND end_date >= ? AND approved = 1
                    ''', (sid, date, date)).fetchone()[0]
                    
                    if leave_check > 0:
                        continue  # 該員工在此日期有請假，跳過
                    
                    # 基本約束檢查
                    if st['shift_counts'].get(date, 0) >= max_per_day:
                        continue
                    if not is_night:
                        if staff_holidays[sid].get(week_of_month) == date:
                            continue
                        if staff_restdays[sid].get(week_of_month) == date:
                            continue
                    
                    # 週班別一致性評分
                    week_consistency_score = 0
                    if week_shift_consistency:
                        current_week_shifts = st['weekly_shifts'][week_of_month]
                        if current_week_shifts:
                            if sid_shift in current_week_shifts:
                                week_consistency_score = 0
                            else:
                                week_consistency_score = 1
                    
                    candidates.append((s, st['count'], st['shift_counts'].get(sid_shift, 0), week_consistency_score))
                
                # 排序候選人
                candidates.sort(key=lambda c: (
                    0 if c[3] == -1 else 1,  # 預先分配最優先
                    c[3] if c[3] != -1 else 0,  # 週班別一致性
                    c[1],  # 總班數
                    c[2]   # 該班別次數
                ))
                
                assigned = candidates[:required]
            
            # 指派並更新狀態
            for c in assigned:
                s = c[0]
                sid = s['staff_id']
                st = staff_status[sid]
                
                st['count'] += 1
                st['shift_counts'][sid_shift] = st['shift_counts'].get(sid_shift, 0) + 1
                st['shift_counts'][date] = st['shift_counts'].get(date, 0) + 1
                st['weekly_shifts'][week_of_month].add(sid_shift)
                st['weekly_hours'][week_of_month] += 8
                st['worked_days'][week_of_month] += 1
                
                conn.execute(
                    '''INSERT INTO schedule
                       (date, shift_id, staff_id, work_hours, is_auto, operator_id, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (date, sid_shift, sid, 8, 1, operator, now_str, now_str)
                )
                worked_today.add(sid)
    
    conn.commit()
    
    # 驗證結果
    is_valid, validation_results = validate_schedule_requirements(
        dates, staff_list, shifts, night_shift_allocations, total_weeks
    )
    
    result = {
        'success': is_valid,
        'validation_results': validation_results,
        'redirect_url': url_for('calendar_view', month=months[0]) if is_valid else None
    }
    
    return result

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
    # 取得查詢參數
    today = datetime.today()
    month = request.args.get('month', today.strftime('%Y-%m'))
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    name = request.args.get('name', '').strip()
    ward = request.args.get('ward', '').strip()
    shift_name = request.args.get('shift_name', '').strip()
    
    # 決定查詢的日期範圍
    if start_date and end_date:
        # 使用自訂日期範圍
        query_start = start_date
        query_end = end_date
        display_month = start_date[:7]  # 用起始日期的年月作為顯示
    else:
        # 使用月份查詢
        year, mon = map(int, month.split('-'))
        days_in_month = monthrange(year, mon)[1]
        query_start = f"{year}-{mon:02d}-01"
        query_end = f"{year}-{mon:02d}-{days_in_month:02d}"
        display_month = month
    
    query = '''
        SELECT schedule.date, shift.name as shift_name, shift.ward as ward,
               COALESCE(staff.name, '缺人值班') as staff_name
        FROM schedule
        JOIN shift ON schedule.shift_id = shift.shift_id
        LEFT JOIN staff ON schedule.staff_id = staff.staff_id
        WHERE schedule.date BETWEEN ? AND ?
    '''
    params = [query_start, query_end]
    
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
    return render_template('calendar_view.html', 
                         month=display_month, 
                         events=events, 
                         name=name, 
                         ward=ward, 
                         shift_name=shift_name,
                         start_date=start_date,
                         end_date=end_date)

@app.route('/staff_schedule_table')
@login_required
def staff_schedule_table():
    today = datetime.today()
    month = request.args.get('month', today.strftime('%Y-%m'))
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    # 決定查詢的日期範圍
    if start_date and end_date:
        # 使用自訂日期範圍
        start_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_obj = datetime.strptime(end_date, '%Y-%m-%d')
        dates = []
        current_date = start_obj
        while current_date <= end_obj:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        display_month = start_date[:7]  # 用起始日期的年月作為顯示
    else:
        # 使用月份查詢
        year, mon = map(int, month.split('-'))
        days_in_month = monthrange(year, mon)[1]
        dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
        display_month = month
    
    # 計算每天的星期
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
    
    # 查詢請假記錄
    leave_records = conn.execute('''
        SELECT staff_id, start_date, end_date, leave_type
        FROM leave_schedule
        WHERE approved = 1 AND 
              ((start_date <= ? AND end_date >= ?) OR 
               (start_date >= ? AND start_date <= ?) OR
               (end_date >= ? AND end_date <= ?))
    ''', (dates[-1], dates[0], dates[0], dates[-1], dates[0], dates[-1])).fetchall()
    
    conn.close()
    
    schedule_map = {}
    for row in schedule:
        schedule_map.setdefault(row['staff_id'], {})[row['date']] = row['shift_name']
    
    # 建立請假對照表
    leave_map = {}
    for leave in leave_records:
        staff_id = leave['staff_id']
        start_date_obj = datetime.strptime(leave['start_date'], '%Y-%m-%d')
        end_date_obj = datetime.strptime(leave['end_date'], '%Y-%m-%d')
        
        # 為請假期間的每一天建立記錄
        current_date = start_date_obj
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str in dates:  # 只處理在查詢範圍內的日期
                if staff_id not in leave_map:
                    leave_map[staff_id] = {}
                leave_map[staff_id][date_str] = leave['leave_type']
            current_date += timedelta(days=1)
    
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
            # 檢查是否有請假記錄
            leave_type = leave_map.get(staff['staff_id'], {}).get(d, '')
            # 檢查是否為星期天
            date_obj = datetime.strptime(d, '%Y-%m-%d')
            is_sunday = date_obj.weekday() == 6
            
            if shift:
                row['shifts'].append(shift)
                row['total_hours'] += 8
            elif leave_type:
                # 有請假記錄
                if is_sunday:
                    # 請假的星期天顯示為例假日
                    row['shifts'].append('')  # 空字串讓前端判斷為例假日
                else:
                    # 平日請假顯示為其他排休
                    row['shifts'].append('其他排休')
            else:
                # 沒有排班也沒有請假，標記為空字串（由前端判斷顯示休息日或例假日）
                row['shifts'].append('')
        table.append(row)
    
    # 傳遞參數給模板
    return render_template('staff_schedule_table.html', 
                         dates=dates, 
                         weekdays=weekdays, 
                         table=table,
                         month=display_month,
                         start_date=start_date,
                         end_date=end_date)

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

# 新增：更新排班偏好
@app.route('/update_staff_preference', methods=['POST'])
@login_required
@admin_required
def update_staff_preference():
    pref_id = request.form['id']
    month = request.form['month']
    staff_id = request.form['staff_id']
    preference_type = request.form['preference_type']
    shift_id_1 = request.form['shift_id_1']
    shift_id_2 = request.form.get('shift_id_2', None)
    week_pattern = request.form.get('week_pattern', None)
    
    conn = get_db_connection()
    try:
        # 更新偏好設定
        conn.execute('''UPDATE staff_preference 
                       SET month = ?, staff_id = ?, preference_type = ?, shift_id_1 = ?, shift_id_2 = ?, week_pattern = ?
                       WHERE id = ?''',
                   (month, staff_id, preference_type, shift_id_1, shift_id_2, week_pattern, pref_id))
        
        conn.commit()
        flash('排班偏好設定已更新', 'success')
    except Exception as e:
        flash(f'更新失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('staff_preference'))

# 新增：假日 On Call 管理頁面
@app.route('/oncall_manage')
@login_required
@admin_required
def oncall_manage():
    conn = get_db_connection()
    staff_list = conn.execute('SELECT * FROM staff ORDER BY staff_id').fetchall()
    
    today = datetime.today()
    default_month = today.strftime('%Y-%m')
    
    # 取得查詢參數
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    staff_filter = request.args.get('staff_filter', '')
    current_month = request.args.get('month', default_month)
    
    calendar_days = []
    sunday_count = 0
    
    if start_date and end_date:
        # 使用日期範圍查詢
        try:
            start_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_obj > end_obj:
                flash('起始日期不能大於結束日期', 'danger')
                start_date = end_date = ''
            elif (end_obj - start_obj).days > 365:
                flash('查詢時間範圍不能超過一年', 'danger')
                start_date = end_date = ''
            else:
                # 產生日期範圍內的所有日期
                current_date = start_obj
                while current_date <= end_obj:
                    date_str = current_date.strftime('%Y-%m-%d')
                    weekday = current_date.strftime('%A')
                    weekday_cn = {'Monday': '一', 'Tuesday': '二', 'Wednesday': '三', 
                                 'Thursday': '四', 'Friday': '五', 'Saturday': '六', 'Sunday': '日'}[weekday]
                    
                    # 只處理星期天
                    if current_date.weekday() == 6:  # 星期天
                        sunday_count += 1
                        
                        # 查詢該日期的 On Call 人員
                        if staff_filter:
                            # 有人員篩選
                            oncall_staff = conn.execute('''
                                SELECT ocs.*, s.name as staff_name
                                FROM oncall_schedule ocs
                                JOIN staff s ON ocs.staff_id = s.staff_id
                                WHERE ocs.date = ? AND ocs.staff_id = ?
                            ''', (date_str, staff_filter)).fetchall()
                        else:
                            # 無人員篩選
                            oncall_staff = conn.execute('''
                                SELECT ocs.*, s.name as staff_name
                                FROM oncall_schedule ocs
                                JOIN staff s ON ocs.staff_id = s.staff_id
                                WHERE ocs.date = ?
                            ''', (date_str,)).fetchall()
                        
                        calendar_days.append({
                            'date': date_str,
                            'weekday': weekday_cn,
                            'is_weekend': True,
                            'oncall_staff': oncall_staff
                        })
                    
                    current_date += timedelta(days=1)
                    
        except ValueError:
            flash('日期格式錯誤', 'danger')
            start_date = end_date = ''
    else:
        # 使用月份查詢（預設模式）
        if staff_filter:
            # 有人員篩選時，使用自定義查詢
            year, month = map(int, current_month.split('-'))
            _, last_day = monthrange(year, month)
            
            for day in range(1, last_day + 1):
                date_str = f"{year:04d}-{month:02d}-{day:02d}"
                date_obj = datetime(year, month, day)
                weekday = date_obj.strftime('%A')
                weekday_cn = {'Monday': '一', 'Tuesday': '二', 'Wednesday': '三', 
                             'Thursday': '四', 'Friday': '五', 'Saturday': '六', 'Sunday': '日'}[weekday]
                
                # 只處理星期天
                if date_obj.weekday() == 6:  # 星期天
                    sunday_count += 1
                    
                    # 查詢該日期的特定人員 On Call 資料
                    oncall_staff = conn.execute('''
                        SELECT ocs.*, s.name as staff_name
                        FROM oncall_schedule ocs
                        JOIN staff s ON ocs.staff_id = s.staff_id
                        WHERE ocs.date = ? AND ocs.staff_id = ?
                    ''', (date_str, staff_filter)).fetchall()
                    
                    calendar_days.append({
                        'date': date_str,
                        'weekday': weekday_cn,
                        'is_weekend': True,
                        'oncall_staff': oncall_staff
                    })
        else:
            # 無人員篩選，使用原本的函數
            calendar_days = generate_calendar_days(current_month)
            # 計算該月星期天數量
            for day in calendar_days:
                if day['weekday'] == '日':
                    sunday_count += 1
    
    conn.close()
    
    return render_template('oncall_manage.html', 
                         staff_list=staff_list,
                         default_month=default_month,
                         current_month=current_month,
                         calendar_days=calendar_days,
                         start_date=start_date,
                         end_date=end_date,
                         staff_filter=staff_filter,
                         sunday_count=sunday_count)

# 新增：新增 On Call 設定
@app.route('/add_oncall', methods=['POST'])
@login_required
@admin_required
def add_oncall():
    date = request.form['date']
    staff_id = request.form['staff_id']
    status = request.form['status']
    
    # 驗證是否為星期天
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        if date_obj.weekday() != 6:  # 星期天是 weekday 6
            flash('只能為星期天設定 On Call！', 'danger')
            return redirect(url_for('oncall_manage'))
    except ValueError:
        flash('日期格式錯誤！', 'danger')
        return redirect(url_for('oncall_manage'))
    
    conn = get_db_connection()
    try:
        # 檢查是否已存在該日期的設定
        existing = conn.execute('SELECT id FROM oncall_schedule WHERE date = ? AND staff_id = ?', 
                               (date, staff_id)).fetchone()
        
        if existing:
            # 更新現有設定
            conn.execute('UPDATE oncall_schedule SET status = ? WHERE date = ? AND staff_id = ?',
                       (status, date, staff_id))
            flash(f'{date} 星期天 On Call 設定已更新', 'success')
        else:
            # 新增設定
            conn.execute('INSERT INTO oncall_schedule (date, staff_id, status) VALUES (?, ?, ?)',
                       (date, staff_id, status))
            flash(f'{date} 星期天 On Call 設定已新增', 'success')
        
        conn.commit()
    except Exception as e:
        flash(f'儲存失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('oncall_manage'))

# 新增：刪除 On Call 設定
@app.route('/delete_oncall', methods=['POST'])
@login_required
@admin_required
def delete_oncall():
    date = request.form['date']
    staff_id = request.form['staff_id']
    
    conn = get_db_connection()
    try:
        # 刪除指定的 On Call 設定
        result = conn.execute('DELETE FROM oncall_schedule WHERE date = ? AND staff_id = ?', 
                             (date, staff_id))
        
        if result.rowcount > 0:
            conn.commit()
            flash(f'{date} 星期天 {staff_id} 的 On Call 設定已刪除', 'success')
        else:
            flash('找不到要刪除的 On Call 設定', 'warning')
            
    except Exception as e:
        flash(f'刪除失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('oncall_manage'))

# 新增：批次設定 On Call
@app.route('/batch_oncall', methods=['POST'])
@login_required
@admin_required
def batch_oncall():
    month = request.form['month']
    oncall_days = int(request.form['oncall_days'])
    
    conn = get_db_connection()
    try:
        # 取得該月所有星期天日期
        sunday_dates = get_weekend_dates(month)  # 現在只返回星期天
        
        # 取得所有人員
        staff_list = conn.execute('SELECT staff_id FROM staff').fetchall()
        
        if not staff_list:
            flash('沒有可分配的人員', 'warning')
            return redirect(url_for('oncall_manage'))
        
        # 為每個星期天分配 On Call 人員（輪流分配）
        for i, date in enumerate(sunday_dates):
            staff_index = i % len(staff_list)
            staff_id = staff_list[staff_index]['staff_id']
            
            # 檢查是否已存在該日期的 On Call 設定
            existing = conn.execute('SELECT id FROM oncall_schedule WHERE date = ?', (date,)).fetchone()
            
            if not existing:
                conn.execute('INSERT INTO oncall_schedule (date, staff_id, status) VALUES (?, ?, ?)',
                           (date, staff_id, 'oncall'))
            print(f"批次設定 {date} 星期天 On Call: {staff_id}")
        
        conn.commit()
        flash(f'批次星期天 On Call 設定已完成，共設定 {len(sunday_dates)} 個星期天', 'success')
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
            'is_weekend': weekday == 'Sunday',  # 只有星期天標記為特殊日期
            'oncall_staff': oncall_staff
        })
    
    return calendar_days

# 輔助函數：取得星期天日期
def get_weekend_dates(month_str):
    """取得指定月份的所有星期天日期"""
    year, month = map(int, month_str.split('-'))
    _, last_day = monthrange(year, month)
    
    sunday_dates = []
    for day in range(1, last_day + 1):
        date = datetime(year, month, day)
        if date.weekday() == 6:  # 星期天是 weekday 6
            sunday_dates.append(date.strftime('%Y-%m-%d'))
    
    return sunday_dates

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

@app.route('/night_shift_allocation')
@login_required
@admin_required
def night_shift_allocation():
    # 預設顯示本月的分配
    today = datetime.now()
    default_start = today.replace(day=1).strftime('%Y-%m-%d')
    default_end = (today.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    default_end = default_end.strftime('%Y-%m-%d')
    
    start_date = request.args.get('start_date', default_start)
    end_date = request.args.get('end_date', default_end)
    
    conn = get_db_connection()
    
    # 取得員工清單
    staff_list = conn.execute('SELECT staff_id, name FROM staff ORDER BY name').fetchall()
    
    # 取得大夜班班別
    night_shifts = conn.execute('''
        SELECT shift_id, name FROM shift 
        WHERE name LIKE '%大夜%' OR name LIKE '%夜班%'
        ORDER BY name
    ''').fetchall()
    
    # 取得指定日期範圍的大夜班預先分配
    allocations = conn.execute('''
        SELECT nsa.*, s.name as staff_name, sh.name as shift_name
        FROM night_shift_allocation nsa
        JOIN staff s ON nsa.staff_id = s.staff_id
        JOIN shift sh ON nsa.shift_id = sh.shift_id
        WHERE (nsa.start_date <= ? AND nsa.end_date >= ?) OR 
              (nsa.start_date >= ? AND nsa.start_date <= ?) OR
              (nsa.end_date >= ? AND nsa.end_date <= ?)
        ORDER BY nsa.start_date, nsa.staff_id
    ''', (end_date, start_date, start_date, end_date, start_date, end_date)).fetchall()
    
    conn.close()
    
    return render_template('night_shift_allocation.html', 
                         staff_list=staff_list, 
                         night_shifts=night_shifts,
                         allocations=allocations,
                         start_date=start_date,
                         end_date=end_date)

@app.route('/add_night_shift_allocation', methods=['POST'])
@login_required
@admin_required
def add_night_shift_allocation():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    staff_id = request.form.get('staff_id')
    shift_id = request.form.get('shift_id')
    
    # 取得查詢參數以便返回
    query_start = request.form.get('query_start_date', start_date)
    query_end = request.form.get('query_end_date', end_date)
    
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT INTO night_shift_allocation 
            (start_date, end_date, staff_id, shift_id)
            VALUES (?, ?, ?, ?)
        ''', (start_date, end_date, staff_id, shift_id))
        conn.commit()
        flash('大夜班預先分配新增成功', 'success')
    except sqlite3.IntegrityError:
        flash('該員工在此時間範圍已有大夜班分配', 'error')
    except Exception as e:
        flash(f'新增失敗：{str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('night_shift_allocation', start_date=query_start, end_date=query_end))

@app.route('/delete_night_shift_allocation', methods=['POST'])
@login_required
@admin_required
def delete_night_shift_allocation():
    allocation_id = request.form.get('allocation_id')
    query_start = request.form.get('query_start_date')
    query_end = request.form.get('query_end_date')
    
    conn = get_db_connection()
    try:
        conn.execute('DELETE FROM night_shift_allocation WHERE id = ?', (allocation_id,))
        conn.commit()
        flash('大夜班預先分配刪除成功', 'success')
    except Exception as e:
        flash(f'刪除失敗：{str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('night_shift_allocation', start_date=query_start, end_date=query_end))

@app.route('/batch_night_shift_allocation', methods=['POST'])
@login_required
@admin_required
def batch_night_shift_allocation():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    shift_id = request.form.get('shift_id')
    
    conn = get_db_connection()
    try:
        # 取得所有員工
        staff_list = conn.execute('SELECT staff_id FROM staff').fetchall()
        
        # 清除該日期範圍的舊分配
        conn.execute('''DELETE FROM night_shift_allocation 
                        WHERE (start_date <= ? AND end_date >= ?) OR 
                              (start_date >= ? AND start_date <= ?) OR
                              (end_date >= ? AND end_date <= ?)''', 
                    (end_date, start_date, start_date, end_date, start_date, end_date))
        
        # 隨機分配大夜班
        staff_ids = [s['staff_id'] for s in staff_list]
        random.shuffle(staff_ids)
        
        # 按週分配（每週 7 天）
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            # 找到這週的開始（週一）
            week_start = current_date - timedelta(days=current_date.weekday())
            week_end = week_start + timedelta(days=6)  # 週日
            
            if len(staff_ids) >= 2:
                # 第一個人：週日-週四
                staff1 = staff_ids.pop()
                part1_start = max(week_start + timedelta(days=6), current_date)  # 週日
                part1_end = min(week_start + timedelta(days=3), end_date_obj)    # 週四
                
                if part1_start <= part1_end and part1_start <= end_date_obj:
                    conn.execute('''
                        INSERT INTO night_shift_allocation 
                        (start_date, end_date, staff_id, shift_id)
                        VALUES (?, ?, ?, ?)
                    ''', (part1_start.strftime('%Y-%m-%d'), part1_end.strftime('%Y-%m-%d'), staff1, shift_id))
                
                # 第二個人：週四-週六
                staff2 = staff_ids.pop() if staff_ids else staff1
                part2_start = max(week_start + timedelta(days=3), current_date)  # 週四
                part2_end = min(week_start + timedelta(days=5), end_date_obj)    # 週六
                
                if part2_start <= part2_end and part2_start <= end_date_obj:
                    conn.execute('''
                        INSERT INTO night_shift_allocation 
                        (start_date, end_date, staff_id, shift_id)
                        VALUES (?, ?, ?, ?)
                    ''', (part2_start.strftime('%Y-%m-%d'), part2_end.strftime('%Y-%m-%d'), staff2, shift_id))
            
            # 移到下週
            current_date = week_end + timedelta(days=1)
        
        conn.commit()
        flash('大夜班批次分配完成', 'success')
    except Exception as e:
        flash(f'批次分配失敗：{str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('night_shift_allocation', start_date=start_date, end_date=end_date))

# 新增：請假管理頁面
@app.route('/leave_manage')
@login_required
@admin_required
def leave_manage():
    # 取得查詢參數
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    staff_filter = request.args.get('staff_filter', '')
    leave_type_filter = request.args.get('leave_type_filter', '')
    
    # 預設顯示本月起往後三個月的請假
    if not start_date:
        today = datetime.now()
        start_date = today.strftime('%Y-%m-%d')
        end_date = (today + timedelta(days=90)).strftime('%Y-%m-%d')
    
    conn = get_db_connection()
    
    # 建立查詢條件
    query = '''
        SELECT ls.*, s.name as staff_name
        FROM leave_schedule ls
        JOIN staff s ON ls.staff_id = s.staff_id
        WHERE 1=1
    '''
    params = []
    
    if start_date and end_date:
        query += ' AND ((ls.start_date <= ? AND ls.end_date >= ?) OR (ls.start_date >= ? AND ls.start_date <= ?))'
        params.extend([end_date, start_date, start_date, end_date])
    
    if staff_filter:
        query += ' AND ls.staff_id = ?'
        params.append(staff_filter)
    
    if leave_type_filter:
        query += ' AND ls.leave_type = ?'
        params.append(leave_type_filter)
    
    query += ' ORDER BY ls.start_date DESC, s.staff_id'
    
    # 取得請假資料
    leaves_raw = conn.execute(query, params).fetchall()
    
    # 轉換為列表並計算請假天數
    leaves = []
    for leave in leaves_raw:
        leave_dict = dict(leave)
        # 計算請假天數（排除星期天）
        start_date_obj = datetime.strptime(leave['start_date'], '%Y-%m-%d')
        end_date_obj = datetime.strptime(leave['end_date'], '%Y-%m-%d')
        
        # 計算總天數
        total_days = (end_date_obj - start_date_obj).days + 1
        
        # 計算期間內的星期天數量
        sunday_count = 0
        current_date = start_date_obj
        while current_date <= end_date_obj:
            if current_date.weekday() == 6:  # 星期天
                sunday_count += 1
            current_date += timedelta(days=1)
        
        # 實際請假天數 = 總天數 - 星期天數量
        leave_days = total_days - sunday_count
        leave_dict['leave_days'] = leave_days
        leave_dict['total_days'] = total_days
        leave_dict['sunday_count'] = sunday_count
        leaves.append(leave_dict)
    
    # 取得員工清單
    staff_list = conn.execute('SELECT staff_id, name FROM staff ORDER BY name').fetchall()
    
    # 請假假別選項
    leave_types = ['事假', '病假', '特休', '婚假', '喪假', '產假', '陪產假', '其他']
    
    conn.close()
    
    return render_template('leave_manage.html', 
                         leaves=leaves,
                         staff_list=staff_list,
                         leave_types=leave_types,
                         start_date=start_date,
                         end_date=end_date,
                         staff_filter=staff_filter,
                         leave_type_filter=leave_type_filter)

# 新增：新增請假記錄
@app.route('/add_leave', methods=['POST'])
@login_required
@admin_required
def add_leave():
    staff_id = request.form['staff_id']
    leave_type = request.form['leave_type']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    reason = request.form.get('reason', '')
    approved = request.form.get('approved', '1') == '1'
    
    # 驗證日期
    try:
        start_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_obj > end_obj:
            flash('起始日期不能大於結束日期', 'danger')
            return redirect(url_for('leave_manage'))
            
    except ValueError:
        flash('日期格式錯誤', 'danger')
        return redirect(url_for('leave_manage'))
    
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT INTO leave_schedule 
            (staff_id, leave_type, start_date, end_date, reason, approved, operator_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (staff_id, leave_type, start_date, end_date, reason, approved, 
              session.get('username', 'system'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        flash('請假記錄新增成功', 'success')
    except Exception as e:
        flash(f'新增失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('leave_manage'))

# 新增：編輯請假記錄
@app.route('/edit_leave', methods=['POST'])
@login_required
@admin_required
def edit_leave():
    leave_id = request.form['leave_id']
    staff_id = request.form['staff_id']
    leave_type = request.form['leave_type']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    reason = request.form.get('reason', '')
    approved = request.form.get('approved', '1') == '1'
    
    # 驗證日期
    try:
        start_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_obj > end_obj:
            flash('起始日期不能大於結束日期', 'danger')
            return redirect(url_for('leave_manage'))
            
    except ValueError:
        flash('日期格式錯誤', 'danger')
        return redirect(url_for('leave_manage'))
    
    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE leave_schedule 
            SET staff_id = ?, leave_type = ?, start_date = ?, end_date = ?, 
                reason = ?, approved = ?, operator_id = ?, updated_at = ?
            WHERE id = ?
        ''', (staff_id, leave_type, start_date, end_date, reason, approved,
              session.get('username', 'system'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              leave_id))
        
        conn.commit()
        flash('請假記錄更新成功', 'success')
    except Exception as e:
        flash(f'更新失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('leave_manage'))

# 新增：刪除請假記錄
@app.route('/delete_leave', methods=['POST'])
@login_required
@admin_required
def delete_leave():
    leave_id = request.form['leave_id']
    
    conn = get_db_connection()
    try:
        conn.execute('DELETE FROM leave_schedule WHERE id = ?', (leave_id,))
        conn.commit()
        flash('請假記錄刪除成功', 'success')
    except Exception as e:
        flash(f'刪除失敗：{str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('leave_manage'))

# 新增：下載請假模板
@app.route('/download_leave_template')
@login_required
@admin_required
def download_leave_template():
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['staff_id', 'leave_type', 'start_date', 'end_date', 'reason'])
    writer.writerow(['N001', '特休', '2024-07-01', '2024-07-03', '休假旅遊'])
    writer.writerow(['N002', '病假', '2024-07-05', '2024-07-05', '身體不適'])
    writer.writerow(['N003', '事假', '2024-07-10', '2024-07-12', '處理私事'])
    output = si.getvalue().encode('utf-8-sig')
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name='leave_template.csv'
    )

# 新增：批次上傳請假資料
@app.route('/upload_leave', methods=['POST'])
@login_required
@admin_required
def upload_leave():
    file = request.files.get('file')
    if not file:
        flash('請選擇檔案', 'danger')
        return redirect(url_for('leave_manage'))
    
    try:
        stream = StringIO(file.stream.read().decode('utf-8-sig'))
        reader = csv.DictReader(stream)
        
        conn = get_db_connection()
        count = 0
        errors = []
        
        for row_num, row in enumerate(reader, start=2):  # 從第2行開始（第1行是標題）
            try:
                staff_id = row.get('staff_id', '').strip()
                leave_type = row.get('leave_type', '').strip()
                start_date = row.get('start_date', '').strip()
                end_date = row.get('end_date', '').strip()
                reason = row.get('reason', '').strip()
                
                # 驗證必要欄位
                if not all([staff_id, leave_type, start_date, end_date]):
                    errors.append(f'第{row_num}行：缺少必要欄位')
                    continue
                
                # 驗證員工是否存在
                staff_exists = conn.execute('SELECT COUNT(*) FROM staff WHERE staff_id = ?', (staff_id,)).fetchone()[0]
                if not staff_exists:
                    errors.append(f'第{row_num}行：員工編號 {staff_id} 不存在')
                    continue
                
                # 驗證日期格式
                try:
                    start_obj = datetime.strptime(start_date, '%Y-%m-%d')
                    end_obj = datetime.strptime(end_date, '%Y-%m-%d')
                    if start_obj > end_obj:
                        errors.append(f'第{row_num}行：起始日期不能大於結束日期')
                        continue
                except ValueError:
                    errors.append(f'第{row_num}行：日期格式錯誤，請使用 YYYY-MM-DD 格式')
                    continue
                
                # 驗證請假假別
                valid_types = ['事假', '病假', '特休', '婚假', '喪假', '產假', '陪產假', '其他']
                if leave_type not in valid_types:
                    errors.append(f'第{row_num}行：無效的請假假別 {leave_type}')
                    continue
                
                # 新增請假記錄
                conn.execute('''
                    INSERT INTO leave_schedule 
                    (staff_id, leave_type, start_date, end_date, reason, approved, operator_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (staff_id, leave_type, start_date, end_date, reason, True,
                      session.get('username', 'system'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                
                count += 1
            except Exception as e:
                errors.append(f'第{row_num}行：{str(e)}')
                continue
        
        conn.commit()
        conn.close()
        
        if count > 0:
            flash(f'成功匯入 {count} 筆請假記錄', 'success')
        if errors:
            flash(f'匯入過程中發生 {len(errors)} 個錯誤：' + '; '.join(errors[:5]), 'warning')
            
    except Exception as e:
        flash(f'檔案讀取失敗：{str(e)}', 'danger')
    
    return redirect(url_for('leave_manage'))

@app.route('/export_staff_schedule_table', methods=['POST'])
@login_required
def export_staff_schedule_table():
    """匯出員工橫式排班表為 CSV"""
    today = datetime.today()
    month = request.form.get('month', today.strftime('%Y-%m'))
    start_date = request.form.get('start_date', '')
    end_date = request.form.get('end_date', '')
    
    # 取得替代代碼設定
    replacement_codes = {
        '早班': request.form.get('morning_shift_code', '').strip(),
        '小夜班': request.form.get('evening_shift_code', '').strip(),
        '大夜班': request.form.get('night_shift_code', '').strip(),
        '例假日': request.form.get('holiday_code', '').strip(),
        '休息日': request.form.get('rest_day_code', '').strip(),
        '其他排休': request.form.get('leave_code', '').strip(),
        '': request.form.get('empty_code', '').strip(),  # 空白替代
    }
    
    # 自訂班別替代
    custom_shift_name = request.form.get('custom_shift_name', '').strip()
    custom_shift_code = request.form.get('custom_shift_code', '').strip()
    if custom_shift_name and custom_shift_code:
        replacement_codes[custom_shift_name] = custom_shift_code
    
    def apply_replacement(text):
        """套用文字替代規則"""
        if not text:  # 處理空白或 None
            return replacement_codes.get('', '') or ''
        
        # 檢查是否有對應的替代代碼
        if text in replacement_codes and replacement_codes[text]:
            return replacement_codes[text]
        
        return text
    
    # 決定查詢的日期範圍
    if start_date and end_date:
        # 使用自訂日期範圍
        start_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_obj = datetime.strptime(end_date, '%Y-%m-%d')
        dates = []
        current_date = start_obj
        while current_date <= end_obj:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        filename_suffix = f"{start_date}_to_{end_date}"
    else:
        # 使用月份查詢
        year, mon = map(int, month.split('-'))
        days_in_month = monthrange(year, mon)[1]
        dates = [f"{year}-{mon:02d}-{day:02d}" for day in range(1, days_in_month+1)]
        filename_suffix = month
    
    # 計算每天的星期
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
    
    # 查詢請假記錄（與staff_schedule_table相同邏輯）
    leave_records = conn.execute('''
        SELECT staff_id, start_date, end_date, leave_type
        FROM leave_schedule
        WHERE approved = 1 AND 
              ((start_date <= ? AND end_date >= ?) OR 
               (start_date >= ? AND start_date <= ?) OR
               (end_date >= ? AND end_date <= ?))
    ''', (dates[-1], dates[0], dates[0], dates[-1], dates[0], dates[-1])).fetchall()
    
    conn.close()
    
    schedule_map = {}
    for row in schedule:
        schedule_map.setdefault(row['staff_id'], {})[row['date']] = row['shift_name']
    
    # 建立請假對照表
    leave_map = {}
    for leave in leave_records:
        staff_id = leave['staff_id']
        start_date_obj = datetime.strptime(leave['start_date'], '%Y-%m-%d')
        end_date_obj = datetime.strptime(leave['end_date'], '%Y-%m-%d')
        
        # 為請假期間的每一天建立記錄
        current_date = start_date_obj
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str in dates:  # 只處理在查詢範圍內的日期
                if staff_id not in leave_map:
                    leave_map[staff_id] = {}
                leave_map[staff_id][date_str] = leave['leave_type']
            current_date += timedelta(days=1)
    
    # 建立 CSV 內容
    si = StringIO()
    writer = csv.writer(si)
    
    # 寫入標題行
    header = ['姓名', '職稱', '總工時', '累積未休時數']
    for i, d in enumerate(dates):
        header.append(f"{d[8:]}({weekdays[i]})")
    writer.writerow(header)
    
    # 寫入資料行
    for staff in staff_list:
        row = [staff['name'], staff['title']]
        total_hours = 0
        leave_hours = 0  # 暫時設為0，可根據需求調整
        
        # 計算總工時並收集排班資料
        shifts_data = []
        for i, d in enumerate(dates):
            shift = schedule_map.get(staff['staff_id'], {}).get(d, '')
            leave_type = leave_map.get(staff['staff_id'], {}).get(d, '')
            
            if shift:
                # 有排班
                processed_shift = apply_replacement(shift)
                shifts_data.append(processed_shift)
                total_hours += 8
            elif leave_type:
                # 有請假記錄
                if weekdays[i] == '日':
                    # 請假的星期天顯示為例假日
                    processed_text = apply_replacement('例假日')
                    shifts_data.append(processed_text)
                else:
                    # 平日請假顯示為其他排休
                    processed_text = apply_replacement('其他排休')
                    shifts_data.append(processed_text)
            else:
                # 根據星期判斷是例假日還是休息日
                if weekdays[i] == '日':
                    processed_text = apply_replacement('例假日')
                    shifts_data.append(processed_text)
                elif weekdays[i] == '六':
                    processed_text = apply_replacement('休息日')
                    shifts_data.append(processed_text)
                else:
                    processed_text = apply_replacement('休息日')
                    shifts_data.append(processed_text)
        
        row.extend([total_hours, leave_hours])
        row.extend(shifts_data)
        writer.writerow(row)
    
    output = si.getvalue().encode('utf-8-sig')
    
    # 根據是否有使用替代代碼來調整檔名
    has_replacement = any(code for code in replacement_codes.values() if code)
    suffix = "_已替代" if has_replacement else ""
    
    return send_file(
        BytesIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'員工橫式排班表_{filename_suffix}{suffix}.csv'
    )

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)