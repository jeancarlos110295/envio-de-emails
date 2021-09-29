const express = require('express')
const axios = require('axios')
const nodemailer = require("nodemailer")
const csv = require('csv-parser')
const fs = require('fs')
const app = express()
require('dotenv').config({ path: './src/.env' })
const { v4 : uuidv4 } = require('uuid')
const csvjson = require ('csvjson')
const QueueBull = require('bull');

const sendMailQueue = new QueueBull('sendMail', {
    redis: {
        host: process.env.HOST_REDIS,
        port: process.env.PORT_REDIS,
        password: process.env.PASSWORD_REDIS
    }
});

app.use(express.json())

const pathInformeCsv = "./src/Informe"

const pathPublicos = {
    testConection : {
        testConection : "/testConection"
    },
    sendEmails : {
        Asociados : "/sendEmailsAsociados",
        GenerarJson : "/GenerarJson"
    }
}

let dataInforme = []

/*async function leerInforme( uuid ){
    if( !fs.existsSync( pathInformeCsv+"/"+uuid+".csv" ) ){
        return null
    }

    const info = fs.readFileSync( pathInformeCsv+"/"+uuid+".csv" , { encoding: 'utf-8' } )
    const data  = JSON.parse( info )
 
    dataInforme = data.informe
}*/

async function guardarInforme( uuid ){
    const payload = {
        informe : dataInforme
    }

    /**
     * CSV
     */
    const dataCsv = csvjson.toCSV(dataInforme, {
        headers: 'key'
    })
    fs.writeFileSync(pathInformeCsv+"/"+uuid+".csv", dataCsv) 
}

app.get(pathPublicos.testConection.testConection, async (request , response) => {
    response.status(200).json('Conexión ok...')
})

const secureMailer = false

let transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: process.env.EMAIL_PORT,
    secure: secureMailer,
    auth: {
        user: process.env.EMAIL_HOST_USER,
        pass: process.env.EMAIL_HOST_PASSWORD
    },
    maxMessages: Infinity
}) 

function sendEmail(dataQueue){
    const {
        to,
        subject,
        bodyEmailText,
        bodyEmailHtml,
        uuid,
        increment
    } = dataQueue

    console.log(`${increment} ) to ${to} subject ${subject}`)

    const setData = (status) => {
        const dateF = new Date()
        const  created_at = `${dateF.getFullYear()}-${ ('0' + dateF.getMonth()).slice(-2) }-${ ('0'+dateF.getDate()).slice(-2) } ${dateF.getHours()}:${dateF.getMinutes()}:${dateF.getSeconds()}`

        dataInforme.push(
            {
                uuid,
                to,
                subject,
                status,
                created_at
            }
        )
    }

    return new Promise( (resolve , reject) => {
        transporter.sendMail({
            from: process.env.EMAIL_FROM,
            to: to,
            subject: subject,
            text: bodyEmailText,
            html: bodyEmailHtml,
        }, (err , info) => {
            if(err){
                setData(false)
                reject(err)
            }else{
                setData(true)
                resolve(info)
            }
        })
    })
}

sendMailQueue.process(async job => {
    return await sendEmail(job.data)
})

sendMailQueue.on('completed',async job => {
  await guardarInforme( job.data.uuid )
})

app.get(pathPublicos.sendEmails.Asociados, async (request , response) => {
    const RequestBody = JSON.parse(JSON.stringify(request.body))

    let results = []
    fs.createReadStream( process.env.RUTA_ARCHVIO_ASOCIADOS )
        .pipe(csv({
            separator: ';',
            headers : false
        }))
        .on('data', (data) => results.push(data))
        .on('end', async () => {
            const uuid = uuidv4()

            await guardarInforme( uuid )

            let increment = 1
            for(let i in results){
                let dataResult = results[i]
                let to = dataResult[0]
                let subject = dataResult[1]

                sendMailQueue.add({
                    to,
                    subject,
                    bodyEmailText: RequestBody.bodyEmailText,
                    bodyEmailHtml: RequestBody.bodyEmailHtml,
                    uuid,
                    increment
                }, {
                    delay: 10
                });

                increment++
            }

            response.status(200).json({
                id_informe : uuid,
                total_emails : increment
            })
        });
})


app.listen( 8080 , () => {
    console.log("Listening App Port: 8080")
})