from pathlib import Path
import tempfile
import os

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from .config import AppConfig
from .engine import MarketHoundEngine
from .updater import AppUpdater, UpdateError

APP_ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.2-hf26"
config_store = AppConfig()
engine = MarketHoundEngine(config_store.values)
updater = AppUpdater(APP_ROOT)


def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 128 * 1024 * 1024

    @app.get('/')
    def index():
        return render_template('index.html', version=VERSION)

    @app.get('/api/state')
    def state():
        snap = engine.snapshot()
        snap["version"] = VERSION
        return jsonify(snap)

    @app.post('/api/configure')
    def configure():
        data = request.get_json(force=True)
        try:
            engine.configure(data.get('ticker','TSLA'), data.get('trade_size',1000), data.get('profit_target',100), data.get('loss_limit',50), data.get('live_mode',False), data.get('debug_capture', None), data.get('execution_mode','PAPER'))
            return jsonify({'ok':True})
        except Exception as e:
            return jsonify({'ok':False,'error':str(e)}), 400

    @app.post('/api/start')
    def start():
        try:
            engine.start(); return jsonify({'ok':True})
        except Exception as e:
            return jsonify({'ok':False,'error':str(e)}), 400

    @app.post('/api/stop')
    def stop():
        try:
            engine.stop()
            return jsonify({'ok':True, 'flat': engine.position == 'FLAT'})
        except Exception as e:
            return jsonify({'ok':False,'error':str(e), 'flat': engine.position == 'FLAT'}), 409

    @app.post('/api/flatten')
    def flatten():
        try:
            state = engine.flatten_now()
            return jsonify({'ok':True, 'flat': True, 'state': state})
        except Exception as e:
            return jsonify({'ok':False,'error':str(e), 'flat': engine.position == 'FLAT'}), 409


    @app.get('/api/debug/status')
    def debug_status():
        info = engine.evidence.status()
        latest = engine.evidence.latest_file()
        return jsonify({
            'ok': True,
            'recording': bool(info.get('enabled')),
            'current_filename': info.get('filename') or '',
            'latest_filename': latest.name if latest else '',
            'latest_size': latest.stat().st_size if latest and latest.exists() else 0,
            'available': bool(latest and latest.exists()),
        })

    @app.get('/api/debug/download')
    def debug_download():
        # "Current / Last" is intentional: an app restart must not make the
        # most recent patrol evidence undiscoverable.
        path = engine.evidence.latest_file()
        debug_root = (APP_ROOT / "data" / "debug").resolve()
        if path is None:
            return jsonify({
                'ok': False,
                'error': 'No evidence file exists yet. Enable DEBUG / EVIDENCE before ARM / START, then run a session.'
            }), 404
        try:
            resolved = path.resolve()
        except Exception:
            return jsonify({'ok':False,'error':'Evidence path could not be resolved.'}), 404
        if not resolved.exists() or not resolved.is_file() or debug_root not in resolved.parents:
            return jsonify({'ok':False,'error':'No valid evidence file is available.'}), 404
        # Flush active recording before serving so the download includes the
        # newest completed record even while the patrol is still running.
        try:
            if engine.evidence._fh is not None:
                engine.evidence._fh.flush()
                os.fsync(engine.evidence._fh.fileno())
        except Exception:
            pass
        return send_file(
            resolved,
            as_attachment=True,
            download_name=resolved.name,
            mimetype='application/x-ndjson',
            conditional=True,
            max_age=0,
        )

    @app.get('/api/setup')
    def setup_get():
        v = config_store.public()
        v.update({
            "version": VERSION,
            "app_root": str(APP_ROOT),
            "running": engine.running,
            "credentials": {"alpaca": engine.alpaca.ready, "openai": engine.ai.ready, "alpaca_live": engine.broker.ready},
        })
        return jsonify(v)

    @app.post('/api/setup')
    def setup_save():
        try:
            if engine.running:
                raise RuntimeError("Stop MarketHound before saving application settings.")
            data = request.get_json(force=True) or {}
            saved = config_store.save(data)
            engine.apply_app_config(saved)
            return jsonify({'ok': True, 'settings': config_store.public()})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 400

    @app.post('/api/broker/test')
    def broker_test():
        try:
            if engine.running:
                raise RuntimeError("Stop MarketHound before testing LIVE account credentials.")
            if not engine.broker.ready:
                raise RuntimeError("Alpaca LIVE trading credentials are not configured.")
            account = engine.broker.account()
            return jsonify({'ok':True,'account':{
                'status':account.get('status',''),
                'account_number_tail':str(account.get('account_number',''))[-4:],
                'equity':account.get('equity','0'),
                'buying_power':account.get('buying_power','0'),
                'trading_blocked':account.get('trading_blocked',False)}})
        except Exception as e:
            return jsonify({'ok':False,'error':str(e)}),400

    @app.get('/api/reports/trades/status')
    def trade_report_status():
        return jsonify({'ok': True, **engine.trade_log.status()})

    @app.get('/api/reports/trades/download')
    def trade_report_download():
        path = engine.trade_log.latest_file()
        if not path or not path.exists():
            return jsonify({'ok': False, 'error': 'No completed trades have been logged yet.'}), 404
        reports_root = (APP_ROOT / 'reports' / 'trades').resolve()
        resolved = path.resolve()
        if reports_root not in resolved.parents:
            return jsonify({'ok': False, 'error': 'Invalid trade-report path.'}), 400
        return send_file(resolved, as_attachment=True, download_name=resolved.name, mimetype='text/csv', conditional=True, max_age=0)

    @app.post('/api/update')
    def update_zip():
        if engine.running:
            return jsonify({'ok':False,'error':'Stop MarketHound before installing an update.'}), 400
        upload = request.files.get('update_zip')
        if upload is None or not upload.filename:
            return jsonify({'ok':False,'error':'Select a MarketHound ZIP package first.'}), 400
        name = secure_filename(upload.filename)
        if not name.lower().endswith('.zip'):
            return jsonify({'ok':False,'error':'Update package must be a .zip file.'}), 400
        try:
            with tempfile.TemporaryDirectory(prefix='markethound-upload-') as td:
                archive = Path(td) / name
                upload.save(archive)
                result = updater.install(archive)
            return jsonify({'ok':True, 'result':result, 'message':'Update installed. Restart MarketHound to load the new code.'})
        except (UpdateError, Exception) as e:
            return jsonify({'ok':False,'error':str(e)}), 400

    return app
