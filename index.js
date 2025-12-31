require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const archiver = require('archiver');

// Configuración desde variables de entorno
const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME
const BACKUP_DIR = process.env.BACKUP_DIR
const DAYS_TO_KEEP = parseInt(process.env.DAYS_TO_KEEP, 10)

// Crear directorio de backups si no existe
if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

async function createBackup() {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = path.join(BACKUP_DIR, `backup-${timestamp}.json`);

    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        console.log('Conectado a MongoDB Atlas');

        const db = client.db(DB_NAME);
        const collections = await db.listCollections().toArray();

        const backup = {
            database: DB_NAME,
            timestamp: new Date().toISOString(),
            collections: {}
        };

        for (const collInfo of collections) {
            const collName = collInfo.name;
            console.log(`Respaldando coleccion: ${collName}`);

            const collection = db.collection(collName);
            const documents = await collection.find({}).toArray();

            backup.collections[collName] = {
                count: documents.length,
                documents: documents
            };
        }

        fs.writeFileSync(backupPath, JSON.stringify(backup, null, 2));
        console.log(`Backup creado: ${backupPath}`);
        console.log(`Total de colecciones: ${collections.length}`);

        return backupPath;
    } catch (error) {
        console.error('Error al crear backup:', error);
        throw error;
    } finally {
        await client.close();
    }
}

async function compressBackup(backupPath) {
    const zipPath = `${backupPath.replace('.json', '')}.zip`;

    return new Promise((resolve, reject) => {
        const output = fs.createWriteStream(zipPath);
        const archive = archiver('zip', { zlib: { level: 9 } });

        output.on('close', () => {
            const sizeInMB = (archive.pointer() / (1024 * 1024)).toFixed(2);
            console.log(`Backup comprimido: ${path.basename(zipPath)} (${sizeInMB} MB)`);
            fs.unlinkSync(backupPath);
            resolve(zipPath);
        });

        archive.on('error', (err) => reject(err));

        archive.pipe(output);
        archive.file(backupPath, { name: path.basename(backupPath) });
        archive.finalize();
    });
}

function cleanOldBackups(daysToKeep = 30) {
    try {
        const files = fs.readdirSync(BACKUP_DIR);
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

        let deletedCount = 0;

        files.forEach(file => {
            if (file.startsWith('backup-') && file.endsWith('.zip')) {
                const filePath = path.join(BACKUP_DIR, file);
                const stats = fs.statSync(filePath);

                if (stats.mtime < cutoffDate) {
                    fs.unlinkSync(filePath);
                    console.log(`Eliminado: ${file}`);
                    deletedCount++;
                }
            }
        });

        if (deletedCount === 0) {
            console.log('No hay backups antiguos para eliminar');
        } else {
            console.log(`Eliminados ${deletedCount} backup(s) antiguo(s)`);
        }
    } catch (error) {
        console.error('Error al limpiar backups antiguos:', error.message);
    }
}

function getBackupStats() {
    try {
        const files = fs.readdirSync(BACKUP_DIR);
        const backupFiles = files.filter(f => f.startsWith('backup-') && f.endsWith('.zip'));

        let totalSize = 0;
        backupFiles.forEach(file => {
            const stats = fs.statSync(path.join(BACKUP_DIR, file));
            totalSize += stats.size;
        });

        const totalSizeMB = (totalSize / (1024 * 1024)).toFixed(2);

        console.log('\nEstadisticas de backups:');
        console.log(`   Total de backups: ${backupFiles.length}`);
        console.log(`   Espacio utilizado: ${totalSizeMB} MB`);
        console.log(`   Ubicación: ${path.resolve(BACKUP_DIR)}`);
    } catch (error) {
        console.error('Error al obtener estadísticas:', error.message);
    }
}

async function main() {
    try {
        console.log('Iniciando proceso de backup...');
        console.log(`Base de datos: ${DB_NAME}`);
        console.log(`Directorio: ${BACKUP_DIR}`);
        console.log(`Retencion: ${DAYS_TO_KEEP} dias\n`);

        const backupPath = await createBackup();
        const zipPath = await compressBackup(backupPath);

        console.log('\nLimpiando backups antiguos...');
        cleanOldBackups(DAYS_TO_KEEP);

        getBackupStats();

        console.log('\nProceso completado exitosamente');
    } catch (error) {
        console.error('\nError en el proceso:', error.message);
        process.exit(1);
    }
}

main();