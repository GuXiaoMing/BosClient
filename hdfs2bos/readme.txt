Dependencies
- Python (>=2.7)
- BCE SDK(0.8.14)
  https://cloud.baidu.com/doc/BOS/Python-SDK.html#.E5.AE.89.E8.A3.85SDK.E5.B7.A5.E5.85.B7.E5.8C.85
- Baidu Hadoop Client
  http://hadoop.baidu.com:8055/wucaishi/tpl/index.html;jsessionid=49F9E81B31C301EA41C25AD44D5E5317

Config
Modify ./conf/all.cfg.template and cp it to ./conf/all.cfg
Ask BOS administrator for the access_key_id and secret_access_key

Entry
./entry/transfer.py

Help
cd entry && python transfer.py -h
