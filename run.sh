sudo systemctl stop article-parser.service
yes | cp /home/article-parser/article-parser.service /usr/lib/systemd/system
systemctl daemon-reload
sudo systemctl start article-parser.service