#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 *  Column format:
 * -------------------------------------------
 * | MAGIC_NUM | len of Column name | Column name | 
 * | data type | max len of char | Column index | nullable | unique |
 * -------------------------------------------
 *
 *
 */

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
	uint32_t num_write_bytes = 0; // 写入 buf 的字节数
	// MAGIC_NUM
	MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
	buf += sizeof(uint32_t);
	num_write_bytes += sizeof(uint32_t);
	// len of Column name
	uint32_t name_length = name_.length();
	MACH_WRITE_TO(uint32_t, buf, name_length);
	buf += sizeof(uint32_t);
	num_write_bytes += sizeof(uint32_t);
	// Column name
	MACH_WRITE_STRING(buf, name_);
	buf += name_length;
	num_write_bytes += name_length;
	// data type
	MACH_WRITE_TO(TypeId, buf, type_);
	buf += sizeof(TypeId);
	num_write_bytes += sizeof(TypeId);
	// if data type == char, write max len of char
	if (type_ == kTypeChar) {
		MACH_WRITE_TO(uint32_t, buf, len_);
		buf += sizeof(uint32_t);
		num_write_bytes += sizeof(uint32_t);
	}
	// Column index
	MACH_WRITE_TO(uint32_t, buf, table_ind_);
	buf += sizeof(uint32_t);
	num_write_bytes += sizeof(uint32_t);
	// nullable
	MACH_WRITE_TO(bool, buf, nullable_);
	buf += sizeof(bool);
	num_write_bytes += sizeof(bool);
	// unique
	MACH_WRITE_TO(bool, buf, unique_);
	buf += sizeof(bool);
	num_write_bytes += sizeof(bool);

	return num_write_bytes;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
	uint32_t size = 0;
	size += sizeof(uint32_t); // MAGIC_NUM
	size += sizeof(uint32_t); // len of Column name
	size += name_.length();   // Column name
	size += sizeof(TypeId);   // data type
	if (type_ == kTypeChar) {
		size += sizeof(uint32_t); // max len of char
	}
	size += sizeof(uint32_t); // Column index
	size += sizeof(bool);     // nullable
	size += sizeof(bool);     // unique
	
	return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
	uint32_t num_read_bytes = 0;
	// MAGIC_NUM
	uint32_t magic_num_read = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);

	if (magic_num_read != COLUMN_MAGIC_NUM) {
		LOG(ERROR) << "In Column::DeserializeFrom magic number match failed." << std::endl;
	}
	// len of Column name
	uint32_t name_length_read = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);
	// Column name
	std::string column_name_read(buf, name_length_read); // directly
	buf += name_length_read;
	num_read_bytes += name_length_read;
	// data type
	TypeId type_read = MACH_READ_FROM(TypeId, buf);
	buf += sizeof(TypeId);
	num_read_bytes += sizeof(TypeId);
	// max len of char
	uint32_t char_length_read = 0;
	if (type_read == kTypeChar) {
		char_length_read = MACH_READ_FROM(uint32_t, buf);
		buf += sizeof(uint32_t);
		num_read_bytes += sizeof(uint32_t);
	}
	// Column index
	uint32_t column_index_read = MACH_READ_FROM(uint32_t, buf);
	buf += sizeof(uint32_t);
	num_read_bytes += sizeof(uint32_t);
	// nullable
	bool nullable_read = MACH_READ_FROM(bool, buf);
	buf += sizeof(bool);
	num_read_bytes += sizeof(bool);
	// unique
	bool unique_read = MACH_READ_FROM(bool, buf);
	buf += sizeof(bool);
	num_read_bytes += sizeof(bool);
	// new column
	if (type_read == kTypeChar) {
		column = new Column(column_name_read, type_read, char_length_read, column_index_read, nullable_read, unique_read);
	} else {
		column = new Column(column_name_read, type_read, column_index_read, nullable_read, unique_read);
	}

	return num_read_bytes;
}
