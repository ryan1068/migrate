package shop

import (
	"bytes"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"math"
	"migrate/model"
	"strconv"
	"sync"
	"time"
)

type SmartFlowImage struct {
	SmartFlowImagesId uint32 `gorm:"column:smart_flow_images_id;type:int(11);PRIMARY_KEY;AUTO_INCREMENT;"`
	AreaId            uint32 `gorm:"column:area_id;type:int(11);DEFAULT:0;NOT NULL;"`
	TrackId           string `gorm:"column:track_id;type:varchar(127);NOT NULL;"`
	AimoFacePicId     uint32 `gorm:"column:aimo_face_pic_id;type:int(11);DEFAULT:0;NOT NULL;"`
	SmartFlowId       uint32 `gorm:"column:smart_flow_id;type:int(11);DEFAULT:0;NOT NULL;"`
	UserMark          string `gorm:"column:user_mark;type:varchar(255);DEFAULT:'';NOT NULL;"`
	CaptureQuality    string `gorm:"column:capture_quality;type:varchar(32);DEFAULT:'';NOT NULL;"`
	BoxSourceId       uint32 `gorm:"column:box_source_id;type:int(11);DEFAULT:0;NOT NULL;"`
	FaceSetsId        uint32 `gorm:"column:face_sets_id;type:int(11);DEFAULT:0;NOT NULL;"`
	Timestamp         uint32 `gorm:"column:timestamp;type:int(11);DEFAULT:0;NOT NULL;"`
	FrameImageId      string `gorm:"column:frame_image_id;type:varchar(32);NOT NULL;"`
	FrameAdvisorId    string `gorm:"column:frame_advisor_id;type:varchar(32);NOT NULL;"`
	XAxis             uint32 `gorm:"column:x_axis;type:int(11);NOT NULL;"`
	YAxis             uint32 `gorm:"column:y_axis;type:int(11);DEFAULT:0;NOT NULL;"`
	FrameWidth        string `gorm:"column:frame_width;type:varchar(32);NOT NULL;"`
	FrameHeight       string `gorm:"column:frame_height;type:varchar(32);NOT NULL;"`
	IsDel             uint8  `gorm:"column:is_del;type:tinyint(4);NOT NULL;"`
	CreatedAt         uint32 `gorm:"column:created_at;type:int(11);DEFAULT:0;NOT NULL;"`
	UpdatedAt         uint32 `gorm:"column:updated_at;type:int(11);DEFAULT:0;NOT NULL;"`
}

// 分表key
var ShardTableKey uint

// 分批查询数据条数
var BatchNum = 300000

// 批量插入到MySQL的条数
var SliceNum = 500

var TableNum = 100

// 库名
func (s SmartFlowImage) DBName() string {
	return "zd_sf"
}

// 新的分表表名
func (s SmartFlowImage) TableName() string {
	if ShardTableKey == 0 {
		panic("必须设置分表key")
	}
	return s.DBName() + ".smart_flow_images_" + strconv.Itoa(int(ShardTableKey))
}

// 设置分表key
func (s *SmartFlowImage) SetShardTableIndex(shardTableKey uint) {
	ShardTableKey = shardTableKey % 100
	return
}

// 原始表名
func (s SmartFlowImage) OriginTableName() string {
	return "4s_wx_db.4s_smart_flow_images"
}

// 获取原始表图片数据
func (s SmartFlowImage) GetOriginImages(shopId uint, offset uint) ([]SmartFlowImage, error) {
	db, err := model.GormOpenDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var smartFlowImages []SmartFlowImage
	err = db.Debug().Table(SmartFlowImage{}.OriginTableName()).
		Where("is_del = 0 AND area_id = ?", shopId).
		Offset(offset).
		Limit(BatchNum).
		Find(&smartFlowImages).
		Error
	if err != nil {
		return nil, err
	}

	return smartFlowImages, nil
}

// 查询原始表总数据量
func (s SmartFlowImage) QueryTotalNum(shopId uint) (uint, error) {
	db, err := model.GormOpenDB()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count uint
	err = db.Debug().Table(SmartFlowImage{}.OriginTableName()).
		Where("is_del = 0 AND area_id = ?", shopId).
		Count(&count).
		Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

// 创建/删除分表
func (s SmartFlowImage) CreateTables(ac string) error {
	db, err := model.GormOpenDB()
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	if err != nil {
		return err
	}
	defer db.Close()

	wg := sync.WaitGroup{}
	wg.Add(TableNum)

	var tables [100]int
	for i, _ := range tables {
		go s.handleTable(db, &wg, ac, i)
	}
	wg.Wait()

	fmt.Printf("创建完成")

	return nil
}

// 操作表
func (s SmartFlowImage) handleTable(db *gorm.DB, wg *sync.WaitGroup, ac string, num int) {
	if ac == "create" {
		db.Exec(fmt.Sprintf("CREATE TABLE zd_sf.smart_flow_images_%d LIKE %s", num, s.OriginTableName()))
	} else if ac == "drop" {
		db.DropTable(fmt.Sprintf("smart_flow_images_%d", num))
	}
	wg.Done()
}

// 批量插入数据
func (s SmartFlowImage) BatchCreate(db *gorm.DB, images []SmartFlowImage, migrateChan chan int) error {
	var buffer bytes.Buffer
	sql := fmt.Sprintf("INSERT INTO %s (`area_id`, `track_id`, `aimo_face_pic_id`, `smart_flow_id`, `user_mark`, `capture_quality`, `box_source_id`, `face_sets_id`, `timestamp`, `frame_image_id`, `frame_advisor_id`, `x_axis`, `y_axis`, `frame_width`, `frame_height`, `is_del`, `created_at`, `updated_at`) VALUES", s.TableName())
	if _, err := buffer.WriteString(sql); err != nil {
		return err
	}
	for i, v := range images {
		if i == len(images)-1 {
			buffer.WriteString(fmt.Sprintf("(%d,'%s',%d, %d,'%s',%s, %d, %d, %d, '%s','%s', %d, %d,'%s', '%s', %d, %d, %d);", v.AreaId, v.TrackId, v.AimoFacePicId, v.SmartFlowId, v.UserMark, v.CaptureQuality, v.BoxSourceId, v.FaceSetsId, v.Timestamp, v.FrameImageId, v.FrameAdvisorId, v.XAxis, v.YAxis, v.FrameHeight, v.FrameHeight, v.IsDel, v.CreatedAt, v.UpdatedAt))
		} else {
			buffer.WriteString(fmt.Sprintf("(%d,'%s',%d, %d,'%s',%s, %d, %d, %d, '%s','%s', %d, %d,'%s', '%s', %d, %d, %d),", v.AreaId, v.TrackId, v.AimoFacePicId, v.SmartFlowId, v.UserMark, v.CaptureQuality, v.BoxSourceId, v.FaceSetsId, v.Timestamp, v.FrameImageId, v.FrameAdvisorId, v.XAxis, v.YAxis, v.FrameHeight, v.FrameHeight, v.IsDel, v.CreatedAt, v.UpdatedAt))
		}
	}

	if err := db.Exec(buffer.String()).Error; err != nil {
		model.Mgoruser().WithFields(logrus.Fields{
			"action": "BatchCreate",
			"error":  err.Error(),
		}).Info("插入数据失败，请查看日志")

		return err
	}

	migrateChan <- 1
	return nil
}

// 旧数据迁移
func (s SmartFlowImage) Migrate() error {
	startTime := time.Now().Unix()

	db, err := model.GormOpenDB()
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	if err != nil {
		return err
	}
	defer db.Close()

	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	shopIds := []uint{1303, 1390, 1301, 1302, 1304, 1305}

	// 按店ID遍历查询数据
	for _, shopId := range shopIds {
		s.SetShardTableIndex(shopId)

		fmt.Printf("开始查询店ID=%d的总数据...\n", shopId)

		// 查询一家店的数据总量
		count, _ := s.QueryTotalNum(shopId)
		if count == 0 {
			continue
		}

		// 防止一家店数据量过大导致内存溢出，分批次导入店数据，每批导入BatchNum条
		batchTimes := math.Ceil(float64(count) / float64(BatchNum))
		fmt.Printf("查询出店ID=%d的总数据为%d条，分%d批导入，每批导入%d条数据\n", shopId, count, int(batchTimes), BatchNum)

		batchIndex := 0
		for i := 0; i < int(count); i += BatchNum {
			images, _ := s.GetOriginImages(shopId, uint(i))
			imagesLen := len(images)

			fmt.Printf("开始导入第%d批%d条数据...\n", batchIndex+1, imagesLen)

			// 动态计算协程数量，把每批的数据量 分组批量插入数据表
			goTimes := math.Ceil(float64(imagesLen) / float64(SliceNum))
			migrateChan := make(chan int, int(goTimes))

			index := 0
			for i := 0; i < imagesLen; i += SliceNum {
				if index == int(goTimes)-1 {
					sliceImages := images[i:imagesLen]
					go s.BatchCreate(db, sliceImages, migrateChan)

				} else {
					sliceImages := images[i : (index+1)*SliceNum]
					go s.BatchCreate(db, sliceImages, migrateChan)
				}
				index++
			}

		Loop:
			for {
				select {
				case <-ticker.C:
					chanLen := len(migrateChan)
					if chanLen == int(goTimes) {
						fmt.Printf("第%d批完成100%s，累计耗时：%ds\n", batchIndex+1, "%", time.Now().Unix()-startTime)
						break Loop
					}

					progressString := fmt.Sprintf("%.2f", float64(chanLen)/float64(int(goTimes)))
					progressFloat, _ := strconv.ParseFloat(progressString, 64)
					progress := int(progressFloat * 100)

					fmt.Printf("已经完成第%d组数据, 当前进度为%d%s...\n", chanLen, progress, "%")
				}
			}

			close(migrateChan)
			batchIndex++
		}

		fmt.Printf("成功导入店ID=%d数据，累计耗时：%ds\n\n", shopId, time.Now().Unix()-startTime)
	}

	fmt.Printf("全部导入完成。\n")

	return nil
}
