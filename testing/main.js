import fs from 'fs/promises';
import { fileTypeFromFile } from 'file-type';
import { createReadStream } from 'node:fs';
import crypto from 'crypto';

const email="test@test.com"
const password = "testing@123"

const BASE_URL="http://localhost:4000"

const FILE_NAME="timer.apk"
const FILE_PATH=`./APK/${FILE_NAME}`
const CHUNK_SIZE_BYTES = 4 * 1024 * 1024


async function getMimeType(filePath) {
  try {
    const type = await fileTypeFromFile(filePath);
    
    if (type) {
      console.log(`MIME type: ${type.mime}`); 
      console.log(`File extension: ${type.ext}`); 
    } else {
      console.log('Could not determine file type (unknown binary).');
    }
    return type.mime
  } catch (error) {
    console.error('Error reading file:', error);
  }
}


async function checkFileSize(filePath) {
  try {
    const stats = await fs.stat(filePath);
    console.log(`File size: ${stats.size} bytes`);
    return stats.size
} catch (error) {
    console.error('File not found or error reading stats:', error.message);
}
}


const getToken= async ()=>{
    try{
        const body = {
            email,
            password
        }

        const res = await fetch(`${BASE_URL}/auth/login`,{
            method:"POST",
            headers:{"Content-Type" : "application/json"},
            body:JSON.stringify(body)

        })
        if (!res.ok) {
        throw new Error(`HTTP error! Status: ${res.status}`);
        }
        const data = await res.json()

        const {access_token:Token} = data
        return Token
}
catch (err){
        console.error("Error",err)
        
    }
}
const create_upload_session=async ()=>{
    try{
        const [mimeType, file_size,access_token] = await Promise.all([
          getMimeType(FILE_PATH),
          checkFileSize(FILE_PATH),
          getToken()
        ]);
        const idempotency_key = crypto.randomUUID();

        const body = {
            filename:FILE_NAME,
            mime:mimeType,
            file_size:file_size,
            idempotency_key
        }

        const res = await fetch(`${BASE_URL}/upload`,{
            method:"POST",
            headers:{
                "Content-Type":"application/json",
                "authorization":`Bearer ${access_token}`
            },
            body:JSON.stringify(body)
        }
        )
        if (!res.ok){
            console.error("Error",res)
        }
        const data = await res.json();
        console.log(data)
//         pub(crate) struct Response {
//     upload_id: Uuid,

//     upload_url: String,

//     sas_expires_at: String,
//     session_expires_at: String,

//     chunk_size_bytes: u64,
//     file_size_bytes: u64,

//     original_filename: String,

//     sanitized_filename: String,

//     #[cfg(debug_assertions)]
//     debug: DebugResponse,
// }
        return {upload_url:data.upload_url,upload_id:data.upload_id}
    }
    catch(err){
        console.err("Error",err)
    }
}
function computeFileSha256(filePath) {
  return new Promise((resolve, reject) => {
    // 1. Create a SHA-256 hash instance
    const hash = crypto.createHash('sha256');
    
    // 2. Create a readable stream from the file
    const stream = createReadStream(filePath);

    // 3. Pipe the file data stream into the hash algorithm
    stream.on('data', (chunk) => {
      hash.update(chunk);
    });

    // 4. Handle stream completion and output the hex digest
    stream.on('end', () => {
      const checksum = hash.digest('hex');
      resolve(checksum);
    });

    // 5. Handle any read errors (e.g., file not found)
    stream.on('error', (err) => {
      reject(err);
    });
  });
}

async function completeUpload(uploadId){
    // 
    try{

        const [checksum,access_token] = await Promise.all([
          computeFileSha256(FILE_PATH),
          getToken()
        ]);

        const body = {
            checksumSha256:checksum
        }
        console.log(body)
        const res = await fetch(`${BASE_URL}/uploads/${uploadId}/complete`,{
            method:"POST",
            headers:{
                "Content-Type":"application/json",
                "authorization":`Bearer ${access_token}`
            },
            body:JSON.stringify(body)
        })
        
        if (!res.ok){
            console.error("Error",res)
        }
        const data = await res.json();

        console.log(data)
    }
    catch(err){
        console.error("Error",err)
    }

}
async function uploadFileWithSas(sasUrl, filePath, contentType = 'application/octet-stream') {
  try {
    const fileBuffer = await fs.readFile(filePath);
    const fileSize = fileBuffer.length;
    const blockCount = Math.ceil(fileSize / CHUNK_SIZE_BYTES);

    console.log(`Starting upload of ${fileSize} bytes in ${blockCount} block(s)...`);

    for (let index = 0; index < blockCount; index += 1) {
      const start = index * CHUNK_SIZE_BYTES;
      const block = fileBuffer.subarray(start, Math.min(start + CHUNK_SIZE_BYTES, fileSize));
      const blockName = `block-${index.toString().padStart(8, '0')}`;
      const blockId = Buffer.from(blockName).toString('base64');
      const blockUrl = new URL(sasUrl);
      blockUrl.searchParams.set('comp', 'block');
      blockUrl.searchParams.set('blockid', blockId);

      const response = await fetch(blockUrl, {
        method: 'PUT',
        headers: {
          'Content-Type': contentType,
          'Content-Length': block.length.toString(),
        },
        body: block,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Block ${blockName} upload failed with status ${response.status}: ${errorText}`,
        );
      }

      console.log(`Uploaded ${blockName} (${block.length} bytes)`);
    }

    console.log('✅ All upload blocks staged successfully!');
    return { success: true };
  } catch (error) {
    console.error('❌ Network or file system error during upload:', error);
    throw error;
  }
}


async function main() {
  const {upload_url,upload_id} = await create_upload_session();

  const responseFromBackend = {
    upload_url,
    content_type: 'application/vnd.android.package-archive',
  };

   await uploadFileWithSas(
    responseFromBackend.upload_url,
    FILE_PATH,
    responseFromBackend.content_type
  );
  await completeUpload(upload_id);
}

main().catch((error) => {
  console.error('Upload failed:', error);
});