from flask import Flask, json, jsonify, request
from flask_cors import CORS, cross_origin
from database import database

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/api/', methods=['GET'])
@cross_origin()
def home() :
    return {
        'code': 200,
        'message' : 'you are in home!'
    }

@app.route('/api/get_biggest_trade', methods=['GET'])
@cross_origin()
def get_biggest_trade():
    _args = request.args
    _database = database()
    _result = _database.get_biggest_trade(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_trade', methods=['GET'])
@cross_origin()
def get_total_trade():
    _args = request.args
    _database = database()
    _result = _database.get_total_trade(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_unique_user', methods=['GET'])
@cross_origin()
def get_total_unique_user():
    _args = request.args
    _database = database()
    _result = _database.get_total_unique_user(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_unique_user', methods=['GET'])
@cross_origin()
def get_total_unique_user_by_address():
    _args = request.args
    _database = database()
    _result = _database.get_total_unique_user_by_address(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_transaction', methods=['GET'])
@cross_origin()
def get_total_transactions():
    _args = request.args
    _database = database()
    _result = _database.get_total_transactions(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_top_10_trader_sov_bridge', methods=['GET'])
@cross_origin()
def get_top_10_trader_sov_bridge():
    _database = database()
    _result = _database.get_top_10_trader_sov_bridge()
    return jsonify(_result)

@app.route('/api/get_top_10_trader_rsk_bridge', methods=['GET'])
@cross_origin()
def get_top_10_trader_rsk_bridge():
    _database = database()
    _result = _database.get_top_10_trader_rsk_bridge()
    return jsonify(_result)

@app.route('/api/get_top_10_trader_moc', methods=['GET'])
@cross_origin()
def get_top_10_trader_moc():
    _database = database()
    _result = _database.get_top_10_trader_moc()
    return jsonify(_result)

@app.route('/api/get_top_10_trader_rif', methods=['GET'])
@cross_origin()
def get_top_10_trader_rif():
    _database = database()
    _result = _database.get_top_10_trader_rif()
    return jsonify(_result)


@app.route('/api/get_total_volume_by_date_moc', methods=['GET'])
@cross_origin()
def get_total_volume_by_date_moc():
    _database = database()
    _result = _database.get_volume_trade_by_date_moc()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_by_date_moc', methods=['GET'])
@cross_origin()
def get_total_unique_user_by_date_moc():
    _database = database()
    _result = _database.get_total_unique_user_by_date_moc()
    return jsonify(_result)

@app.route('/api/get_new_user_by_date_moc', methods=['GET'])
@cross_origin()
def get_new_user_by_date_moc():
    _database = database()
    _result = _database.get_new_user_by_date_moc()
    return jsonify(_result)

@app.route('/api/get_volume_trade_by_date_rif', methods=['GET'])
@cross_origin()
def get_volume_trade_by_date_rif():
    _database = database()
    _result = _database.get_volume_trade_by_date_rif()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_by_date_rif', methods=['GET'])
@cross_origin()
def get_total_unique_user_by_date_rif():
    _database = database()
    _result = _database.get_total_unique_user_by_date_rif()
    return jsonify(_result)

@app.route('/api/get_new_user_by_date_rif', methods=['GET'])
@cross_origin()
def get_new_user_by_date_rif():
    _database = database()
    _result = _database.get_new_user_by_date_rif()
    return jsonify(_result)

@app.route('/api/get_top_10_trader_by_address', methods=['GET'])
@cross_origin()
def get_top_10_trader_by_address():
    _args = request.args
    _database = database()
    _result = _database.get_top_10_trader_by_address(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_trade_volume_monthly', methods=['GET'])
@cross_origin()
def get_total_trade_volume_monthly():
    _args = request.args
    _database = database()
    _result = _database.get_total_trade_volume_monthly(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_unique_user_monthly', methods=['GET'])
@cross_origin()
def get_total_unique_user_monthly():
    _args = request.args
    _database = database()
    _result = _database.get_total_unique_user_monthly(_args['database_name'])
    return jsonify(_result)

@app.route('/api/get_total_new_user_monthly', methods=['GET'])
@cross_origin()
def get_total_new_user_monthly():
    _args = request.args
    _database = database()
    _result = _database.get_total_new_user_monthly(_args['database_name'])
    return jsonify(_result)


@app.route('/api/get_best_trader_rsk', methods=['GET'])
@cross_origin()
def get_best_trader_rsk():
    _database = database()
    _result = _database.get_best_trader_rsk()
    return jsonify(_result)

@app.route('/api/get_total_trade_volume_rsk', methods=['GET'])
@cross_origin()
def get_total_trade_volume_rsk():
    _database = database()
    _result = _database.get_total_trade_volume_rsk()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_rsk', methods=['GET'])
@cross_origin()
def get_total_unique_user_rsk():
    _database = database()
    _result = _database.get_total_unique_user_rsk()
    return jsonify(_result)

@app.route('/api/get_collected_fee_rsk', methods=['GET'])
@cross_origin()
def get_collected_fee_rsk():
    _database = database()
    _result = _database.get_collected_fee_rsk()
    return jsonify(_result)

@app.route('/api/get_top_10_address_rsk', methods=['GET'])
@cross_origin()
def get_top_10_address_rsk():
    _database = database()
    _result = _database.get_top_10_address_rsk()
    return jsonify(_result)

@app.route('/api/get_total_trade_by_date_rsk', methods=['GET'])
@cross_origin()
def get_total_trade_by_date_rsk():
    _database = database()
    _result = _database.get_total_trade_by_date_rsk()
    return jsonify(_result)

@app.route('/api/get_total_trade_by_month_rsk', methods=['GET'])
@cross_origin()
def get_total_trade_by_month_rsk():
    _database = database()
    _result = _database.get_total_trade_by_month_rsk()
    return jsonify(_result)

@app.route('/api/get_total_collected_fee_by_date_rsk', methods=['GET'])
@cross_origin()
def get_total_collected_fee_by_date_rsk():
    _database = database()
    _result = _database.get_total_collected_fee_by_date_rsk()
    return jsonify(_result)


@app.route('/api/get_total_collected_fee_by_month_rsk', methods=['GET'])
@cross_origin()
def get_total_collected_fee_by_month_rsk():
    _database = database()
    _result = _database.get_total_collected_fee_by_month_rsk()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_by_date_rsk', methods=['GET'])
@cross_origin()
def get_total_unique_user_by_date_rsk():
    _database = database()
    _result = _database.get_total_unique_user_by_date_rsk()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_by_month_rsk', methods=['GET'])
@cross_origin()
def get_total_unique_user_by_month_rsk():
    _database = database()
    _result = _database.get_total_unique_user_by_month_rsk()
    return jsonify(_result)

@app.route('/api/get_best_trade_sov', methods=['GET'])
@cross_origin()
def get_best_trade_sov():
    _database = database()
    _result = _database.get_best_trade_sov()
    return jsonify(_result)

@app.route('/api/get_total_trade_sovryn', methods=['GET'])
@cross_origin()
def get_total_trade_sov():
    _database = database()
    _result = _database.get_total_trade_sov()
    return jsonify(_result)

@app.route('/api/get_total_unique_user_sovryn', methods=['GET'])
@cross_origin()
def get_total_unique_user_sovryn():
    _database = database()
    _result = _database.get_total_unique_user_sovryn()
    return jsonify(_result)

@app.route('/api/get_total_fee_sovryn', methods=['GET'])
@cross_origin()
def get_total_fee_sovryn():
    _database = database()
    _result = _database.get_total_fee_sovryn()
    return jsonify(_result)

@app.route('/api/get_top_trader_sov', methods=['GET'])
@cross_origin()
def get_top_trader_sov():
    _database = database()
    _result = _database.get_top_trader_sov()
    return jsonify(_result)

@app.route('/api/get_total_trade_by_date_sov', methods=['GET'])
@cross_origin()
def get_total_trade_by_date_sov():
    _database = database()
    _result = _database.get_total_trade_by_date_sov()
    return jsonify(_result)

@app.route('/api/get_total_trade_by_month_sov', methods=['GET'])
@cross_origin()
def get_total_trade_by_month_sov():
    _database = database()
    _result = _database.get_total_trade_by_month_sov()
    return jsonify(_result)

@app.route('/api/get_collected_fee_sov_date', methods=['GET'])
@cross_origin()
def get_collected_fee_sov_date():
    _database = database()
    _result = _database.get_collected_fee_sov_date()
    return jsonify(_result)

@app.route('/api/get_collected_fee_sov_month', methods=['GET'])
@cross_origin()
def get_collected_fee_sov_month():
    _database = database()
    _result = _database.get_collected_fee_sov_month()
    return jsonify(_result)

@app.route('/api/get_total_unique_address_by_date_sov', methods=['GET'])
@cross_origin()
def get_total_unique_address_by_date_sov():
    _database = database()
    _result = _database.get_total_unique_address_by_date_sov()
    return jsonify(_result)

@app.route('/api/get_total_unique_address_by_month_sov', methods=['GET'])
@cross_origin()
def get_total_unique_address_by_month_sov():
    _database = database()
    _result = _database.get_total_unique_address_by_month_sov()
    return jsonify(_result)

@app.route('/api/search_address_moc', methods=['GET'])
@cross_origin()
def search_address_moc():
    _args = request.args
    _database = database()
    _result = _database.search_address_moc(_args['address'])
    return jsonify(_result)

@app.route('/api/search_address_rif', methods=['GET'])
@cross_origin()
def search_address_rif():
    _args = request.args
    _database = database()
    _result = _database.search_address_rif(_args['address'])
    return jsonify(_result)

@app.route('/api/search_address_rsk', methods=['GET'])
@cross_origin()
def search_address_rsk():
    _args = request.args
    _database = database()
    _result = _database.search_address_rsk(_args['address'])
    return jsonify(_result)


@app.route('/api/search_address_sov', methods=['GET'])
@cross_origin()
def search_address_sov():
    _args = request.args
    _database = database()
    _result = _database.search_address_sov(_args['address'])
    return jsonify(_result)

if __name__ == '__main__':
    app.run()