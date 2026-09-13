from flask import Flask, jsonify, render_template, request, session

app = Flask(__name__)
app.config['SECRET_KEY'] = 'flask-basics-secret'

app.json.ensure_ascii = False


@app.route('/')
def index():
    return render_template('demo_basics.html')


@app.route('/hello')
def hello():
    return 'Hello Flask'


@app.route('/methods', methods=['GET', 'POST', 'PUT', 'DELETE'])
def methods():
    return jsonify({"method": request.method})


# ================= 路径参数 =================
@app.route('/user/<name>')
def user(name):
    return jsonify({"name": name})


@app.route('/item/<int:item_id>')
def item(item_id):
    # 传非整数时 <int:> 匹配失败，直接返回 404
    return jsonify({"item_id": item_id, "next": item_id + 1})


# ================= 查询参数 =================
@app.route('/search')
def search():
    return jsonify({
        "q": request.args.get('q', ''),                 # 取不到时用默认值
        "page": request.args.get('page', 1, type=int),  # 直接转成 int
        "tags": request.args.getlist('tag'),            # 同名参数取多个
    })


# ================= 请求体 =================
@app.route('/json', methods=['POST'])
def json_body():
    data = request.get_json(silent=True)   # silent=True：解析失败返回 None，不抛异常
    if data is None:
        return jsonify({"error": "请求体不是合法 JSON"}), 400
    return jsonify({"received": data})


@app.route('/form', methods=['POST'])
def form_body():
    return jsonify({
        "name": request.form.get('name'),
        "age": request.form.get('age', type=int),
    })


# ================= Session =================
@app.route('/login', methods=['POST'])
def login():
    session['user'] = request.form.get('user', 'guest')
    return jsonify({"login": session['user']})


@app.route('/me')
def me():
    return jsonify({"user": session.get('user', '未登录')})


@app.route('/logout')
def logout():
    session.clear()
    return jsonify({"msg": "已退出"})


@app.errorhandler(404)
def not_found(e):
    # 404 也返回 JSON，方便接口调试
    return jsonify({"error": "资源不存在", "path": request.path}), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True, use_reloader=False)
