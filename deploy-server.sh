#!/bin/bash
echo "Pushing to Git..."
git add .
git commit -m "Server update $(date)"
git push

echo "Uploading server.js to IONOS..."
scp /Users/fahim/Desktop/fuelApp/fuel-proxy/server.js root@185.230.216.185:/root/fuel-proxy/server.js

echo "Restarting server..."
ssh root@185.230.216.185 "pm2 restart fuelcompare-api"

echo "Done! Server restarted."
