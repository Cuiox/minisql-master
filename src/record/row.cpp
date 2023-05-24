#include "record/row.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *
 */

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
	uint32_t num_write_bytes = 0; // 写入 buf 的字节数

	// Field Nums
	uint32_t num_fields = fields_.size();
	MACH_WRITE_TO(uint32_t, buf, num_fields);
	buf += sizeof(uint32_t); // 移动 buf 指针
	num_write_bytes += sizeof(uint32_t); // 增加写入的字节数
	// Null bitmap
	bool null_bitmap[num_fields] = {0};
	for (uint32_t i = 0; i < num_fields; i++) {
		null_bitmap[i] = fields_[i]->IsNull();
		// if (fields_[i]->IsNull()) {
		// 	null_bitmap[i] = 1;
		// } else {
		// 	null_bitmap[i] = 0;
		// }
	}
	MACH_WRITE_TO(bool*, buf, null_bitmap);
	buf += num_fields * sizeof(bool);
	num_write_bytes += num_fields * sizeof(bool);
	// fields
	for (uint32_t i = 0; i < num_fields; i++) { // 最后一个个写
		fields_[i]->SerializeTo(buf);
		uint32_t field_size = fields_[i]->GetSerializedSize();
		buf += field_size;
		num_write_bytes += field_size;
	}

	return num_write_bytes;
}

/**
 * TODO: Student Implement
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
	uint32_t num_read_bytes = 0;
	// Field Nums
	uint32_t num_fields = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);
	// Null bitmap
	bool null_bitmap[num_fields] = {0};
	for (uint32_t i = 0; i < num_fields; i++) {
		null_bitmap[i] = MACH_READ_FROM(bool, buf);
		buf += sizeof(bool);
	}
	num_read_bytes += num_fields * sizeof(bool);
	// fields
	for (uint32_t i = 0; i < num_fields; i++) {
		Field* f = nullptr;
		//f->DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &f, null_bitmap[i]);
		if (null_bitmap[i] == true) {
			f->DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &f, true);
		} else {
			f->DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &f, false);
		}
		fields_.push_back(f);
		uint32_t field_size = f->GetSerializedSize();
		buf += field_size;
		num_read_bytes += field_size;
	}

	return num_read_bytes;
}

/**
 * TODO: Student Implement
 */
uint32_t Row::GetSerializedSize(Schema *schema) const {
	uint32_t size = 0;
	size += sizeof(uint32_t); // Field Nums
	uint32_t num_fields = fields_.size();
	size += num_fields * sizeof(bool); // Null bitmap
	
	for (uint32_t i = 0; i < num_fields; i++) { // fields
		size += fields_[i]->GetSerializedSize();
	}
	return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
