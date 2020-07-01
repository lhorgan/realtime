const puppeteer = require('puppeteer');
const lineByLine = require('n-readlines');
const request = require("request")

class I {
    constructor(ifname, serverURL) {
        (async () => {
            this.serverURL = serverURL;
            this.browser = await puppeteer.launch({args: ['--no-sandbox']});
            this.ids = this.readIds(ifname);
            await this.processIds();
            this.browser.close();
        })();
    }

    async processIds() {
        for(let i = 1000; i < 2000; i++) {
            let page = await this.browser.newPage();
            await page.goto(`https://twitter.com/i/user/${this.ids[i]}`, {"waitUntil" : "networkidle0"});
            let url = await page.url();
            let username = url.split("/").pop();
            console.log(username);
            
            request.get(`${this.serverURL}/username/?username=${username}`, (err, res, body) => {
                if(err || res.statusCode > 200) {
                    console.log("Something went wrong...");
                    console.log(err);
                }
                else {
                    console.log(this.ids[i] + "\t" + username);
                }
            });

            page.close();
            await this.rest(parseInt(20000 + Math.random() * 20000));
        }
    }

    async rest(ms) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve();
            }, ms);
        });
    }

    readIds(ifname) {
        let list = [];
        let readstream = new lineByLine(ifname);
        
        while(true) {
            let line = readstream.next();
            if(!line) break;
            line = line.toString("ascii");
            list.push(line.split("\t")[0]);
        }
    
        return list;
    }
}

let e = new I("/home/luke/Documents/lazer/id_to_username/latest_sinceid_file_high_active.tsv", "http://3.80.160.1:5000");