#include "record/schema.h"

/**
 *  Schema format:
 * -------------------------------------------
 * | Header | Column-1 | ... | Column-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | SCHEMA_MAGIC_NUM | Column Nums |
 * -------------------------------------------
 *
 *
 */

/**
 * TODO: Student Implement
 * Schema to buf
 */
uint32_t Schema::SerializeTo(char *buf) const {
	uint32_t num_write_bytes = 0; // 写入 buf 的字节数

	MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM); // 先写入魔数
	buf += sizeof(uint32_t); // 移动 buf 指针
	num_write_bytes += sizeof(uint32_t); // 增加写入的字节数

	uint32_t num_columns = columns_.size();
	MACH_WRITE_TO(uint32_t, buf, num_columns); // 再写 Column Nums
	buf += sizeof(uint32_t); // 移动 buf 指针
	num_write_bytes += sizeof(uint32_t); // 增加写入的字节数

	for (uint32_t i = 0; i < num_columns; i++) { // 最后一个个写
		columns_[i]->SerializeTo(buf);
		uint32_t column_size = columns_[i]->GetSerializedSize();
		buf += column_size;
		num_write_bytes += column_size;
	}

	return num_write_bytes;
}

uint32_t Schema::GetSerializedSize() const {
	uint32_t size = 0;
	size += 2 * sizeof(uint32_t);
	uint32_t num_columns = columns_.size();
	for (uint32_t i = 0; i < num_columns; i++) { // 最后一个个写
		size += columns_[i]->GetSerializedSize();
	}
	return size;
}

/**
 * TODO: Student Implement
 * buf to Schema
 */
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
	uint32_t num_read_bytes = 0;
	// magic num
	uint32_t magic_num_read = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);

	if (magic_num_read != SCHEMA_MAGIC_NUM) {
		LOG(ERROR) << "In Schema::DeserializeFrom magic number match failed." << std::endl;
	}
	// num columns
	uint32_t num_columns_read = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);
	// columns
	std::vector<Column *> column_vector;
	for (uint32_t i = 0; i < num_columns_read; i++) {
		Column* c = nullptr;
		c->DeserializeFrom(buf, c);
		column_vector.push_back(c);
		uint32_t column_size = c->GetSerializedSize();
		buf += column_size;
		num_read_bytes += column_size;
	}
	// new schema
	schema = new Schema(column_vector);

	return num_read_bytes;
}