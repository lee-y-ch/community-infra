import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import sharp from "sharp";

const s3 = new S3Client({});
const processedBucket = process.env.PROCESSED_BUCKET;

const ALLOWED_CATEGORIES = new Set(["post", "profile"]);

function createProcessedKey(sourceKey) {
  const lastSlashIndex = sourceKey.lastIndexOf("/");
  const lastDotIndex = sourceKey.lastIndexOf(".");

  const keyWithoutExtension =
    lastDotIndex > lastSlashIndex
      ? sourceKey.slice(0, lastDotIndex)
      : sourceKey;

  return `${keyWithoutExtension}.webp`;
}

async function bodyToBuffer(body) {
  if (!body) {
    throw new Error("S3 객체 응답에 Body가 없습니다.");
  }

  const bytes = await body.transformToByteArray();
  return Buffer.from(bytes);
}

async function processImage(sourceBuffer, category) {
  const image = sharp(sourceBuffer, {
    failOn: "error",
  }).rotate();

  if (category === "profile") {
    return image
      .resize({
        width: 512,
        fit: "inside",
        withoutEnlargement: true,
      })
      .webp({
        quality: 82,
        effort: 4,
      })
      .toBuffer();
  }

  return image
    .resize({
      width: 1600,
      fit: "inside",
      withoutEnlargement: true,
    })
    .webp({
      quality: 82,
      effort: 4,
    })
    .toBuffer();
}

async function processS3Record(record) {
  if (record.eventSource !== "aws:s3") {
    console.log("S3 이벤트가 아니므로 건너뜁니다.");
    return null;
  }

  const sourceBucket = record.s3.bucket.name;
  const sourceKey = decodeURIComponent(
      record.s3.object.key.replace(/\+/g, " ")
  );

  const category = sourceKey.split("/")[0];

  if (!ALLOWED_CATEGORIES.has(category)) {
    console.log(
        JSON.stringify({
          message: "처리 대상 Prefix가 아니므로 건너뜁니다.",
          sourceKey,
        })
    );
    return null;
  }

  console.log(
      JSON.stringify({
        message: "이미지 처리를 시작합니다.",
        sourceBucket,
        sourceKey,
      })
  );

  const originalObject = await s3.send(
      new GetObjectCommand({
        Bucket: sourceBucket,
        Key: sourceKey,
      })
  );

  const sourceBuffer = await bodyToBuffer(originalObject.Body);
  const processedBuffer = await processImage(
      sourceBuffer,
      category
  );
  const processedKey = createProcessedKey(sourceKey);

  await s3.send(
      new PutObjectCommand({
        Bucket: processedBucket,
        Key: processedKey,
        Body: processedBuffer,
        ContentType: "image/webp",
        CacheControl: "public, max-age=31536000, immutable",
      })
  );

  const result = {
    sourceBucket,
    sourceKey,
    processedBucket,
    processedKey,
    originalBytes: sourceBuffer.length,
    processedBytes: processedBuffer.length,
  };

  console.log(JSON.stringify(result));
  return result;
}

export const handler = async (event) => {
  if (!processedBucket) {
    throw new Error(
        "PROCESSED_BUCKET 환경변수가 설정되지 않았습니다."
    );
  }

  const records = event?.Records ?? [];
  const isSqsEvent = records.some(
      (record) => record.eventSource === "aws:sqs"
  );

  // 현재 운영 중인 S3 직접 호출 경로
  if (!isSqsEvent) {
    const results = [];

    for (const record of records) {
      const result = await processS3Record(record);

      if (result) {
        results.push(result);
      }
    }

    return {
      processedCount: results.length,
      results,
    };
  }

  // 앞으로 사용할 SQS 호출 경로
  const batchItemFailures = [];

  for (const sqsRecord of records) {
    try {
      const s3Records =
          extractS3RecordsFromSqsRecord(sqsRecord);

      for (const s3Record of s3Records) {
        await processS3Record(s3Record);
      }
    } catch (error) {
      console.error(
          JSON.stringify({
            message: "SQS 이미지 처리에 실패했습니다.",
            messageId: sqsRecord.messageId,
            receiveCount:
            sqsRecord.attributes?.ApproximateReceiveCount,
            error: error.message,
          })
      );

      batchItemFailures.push({
        itemIdentifier: sqsRecord.messageId,
      });
    }
  }

  return {
    batchItemFailures,
  };
};

export function extractS3RecordsFromSqsRecord(sqsRecord) {
  const message = JSON.parse(sqsRecord.body);

  // S3 알림 설정 시 전달될 수 있는 연결 확인 이벤트
  if (message.Event === "s3:TestEvent") {
    return [];
  }

  if (!Array.isArray(message.Records)) {
    throw new Error(
        "SQS 메시지에 유효한 S3 Records가 없습니다."
    );
  }

  return message.Records;
}