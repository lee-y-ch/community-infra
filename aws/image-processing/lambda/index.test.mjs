import test from "node:test";
import assert from "node:assert/strict";

import {
    extractS3RecordsFromSqsRecord,
} from "./index.mjs";

test("SQS 메시지에서 S3 이벤트 레코드를 추출한다", () => {
    const s3Record = {
        eventSource: "aws:s3",
        s3: {
            bucket: {
                name: "original-bucket",
            },
            object: {
                key: "post/example.jpg",
            },
        },
    };

    const sqsRecord = {
        body: JSON.stringify({
            Records: [s3Record],
        }),
    };

    const result =
        extractS3RecordsFromSqsRecord(sqsRecord);

    assert.deepEqual(result, [s3Record]);
});