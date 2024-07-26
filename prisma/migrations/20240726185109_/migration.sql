-- CreateTable
CREATE TABLE "Tasks" (
    "id" TEXT NOT NULL,
    "command" TEXT NOT NULL,
    "scheduledAt" TIMESTAMP(3) NOT NULL,
    "pickedAt" TIMESTAMP(3),
    "startedAt" TIMESTAMP(3),
    "completedAt" TIMESTAMP(3),
    "failedAt" TIMESTAMP(3),

    CONSTRAINT "Tasks_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Tasks_id_key" ON "Tasks"("id");

-- CreateIndex
CREATE INDEX "Tasks_scheduledAt_idx" ON "Tasks"("scheduledAt");
